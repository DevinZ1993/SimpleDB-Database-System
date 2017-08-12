package simpledb;

import java.util.HashMap;
import java.util.Map;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int ioCostPerPage;
    private int cardinality = 0;
    private int numPages;
    private final Map<Integer,IntHistogram> intHistograms = new HashMap<>();
    private final Map<Integer,StringHistogram> stringHistograms = new HashMap<>();

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here

        this.ioCostPerPage = ioCostPerPage;

        final DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
    	final TupleDesc tupleDesc = dbFile.getTupleDesc();
    	Map<Integer,Integer> mins = new HashMap<>();
    	Map<Integer,Integer> maxs = new HashMap<>();
    	for (int i=0; i<tupleDesc.numFields(); i++) {
    	    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
    	        mins.put(i, Integer.MAX_VALUE);
    	        maxs.put(i, Integer.MIN_VALUE);
            } else {
    	        stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

    	DbFileIterator itr = dbFile.iterator(new TransactionId());
    	try {
    	    itr.open();
    	    try {
                while (itr.hasNext()) {
                    final Tuple tuple = itr.next();
                    for (int i=0; i<tupleDesc.numFields(); i++) {
                        if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                            IntField field = (IntField)tuple.getField(i);
                            if (field.getValue() < mins.get(i)) {
                                mins.put(i, field.getValue());
                            }
                            if (field.getValue() > maxs.get(i)) {
                                maxs.put(i, field.getValue());
                            }
                        }
                    }
                    cardinality++;
                }
            } finally {
    	        itr.close();
            }


            for (Integer key : mins.keySet()) {
    	        if (mins.get(key) <= maxs.get(key)) {
                    intHistograms.put(key, new IntHistogram(NUM_HIST_BINS, mins.get(key), maxs.get(key)));
                } else {
                    intHistograms.put(key, new IntHistogram(NUM_HIST_BINS, Integer.MIN_VALUE, Integer.MAX_VALUE));
                }

            }

    	    itr.rewind();
    	    try {
                while (itr.hasNext()) {
                    final Tuple tuple = itr.next();
                    for (Integer idx : intHistograms.keySet()) {
                        intHistograms.get(idx).addValue(((IntField)tuple.getField(idx)).getValue());
                    }
                    for (Integer idx : stringHistograms.keySet()) {
                        stringHistograms.get(idx).addValue(((StringField)tuple.getField(idx)).getValue());
                    }
                }
            } finally {
    	        itr.close();
            }
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException(e);
        }

        final int pageSize = BufferPool.getPageSize();
        this.numPages = (cardinality*tupleDesc.getSize()+pageSize-1)/pageSize;
    }


    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	// some code goes here
        return ioCostPerPage * numPages;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	// some code goes here
        return (int) (selectivityFactor*cardinality);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	// some code goes here
        if (stringHistograms.containsKey(field)) {
            return stringHistograms.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        } else {
            return intHistograms.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }
    }

}

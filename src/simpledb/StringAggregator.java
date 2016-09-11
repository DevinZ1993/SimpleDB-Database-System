package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private final int gbfield;
	private final TupleDesc desc;
	private final Map<Field,Integer> cnts;
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // Done
    	this.gbfield = gbfield;
    	desc = (NO_GROUPING == gbfield)?
    			new TupleDesc(new Type[]{Type.INT_TYPE}):
    			new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    	cnts = new HashMap<Field,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // Done
    	Field key = (gbfield == NO_GROUPING)?
    			DUMMY_FIELD : tup.getField(gbfield);
    	
    	if (null != key) {
    		if (cnts.containsKey(key)) {
    			cnts.put(key, cnts.get(key)+1);
    		} else {
    			cnts.put(key, 1);
    		}
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // Done
    	return new DbIterator() {
    		private Iterator<Field> child;
    		
			@Override
			public void open() throws DbException, TransactionAbortedException {
				child = cnts.keySet().iterator();
			}
			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				return null != child && child.hasNext();
			}
			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				Tuple tup = new Tuple(desc);
				Field key = child.next();
				
				if (1 == desc.numFields()) {
					tup.setField(0, new IntField(cnts.get(key)));
				} else {
					tup.setField(0, key);
					tup.setField(1, new IntField(cnts.get(key)));
				}
				return tup;
			}
			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				child = cnts.keySet().iterator();
			}
			@Override
			public TupleDesc getTupleDesc() {
				return desc;
			}
			@Override
			public void close() {
				child = null;
			}
    	};
    }

}

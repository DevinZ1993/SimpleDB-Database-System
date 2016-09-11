package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

	private final int gbfield;
	private final int afield;
	private final Op op;
    private final TupleDesc desc;
	private final Map<Field, Integer> vals;
	private final Map<Field, Integer> cnts;
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // Done
    	this.gbfield = gbfield;
    	this.afield = afield;
    	op = what;
        desc = (NO_GROUPING == gbfield)?
        		new TupleDesc(new Type[]{Type.INT_TYPE}):
        		new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
    	vals = new HashMap<Field,Integer>();
    	cnts = new HashMap<Field,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // Done
    	Field key = (NO_GROUPING == gbfield)?
    			DUMMY_FIELD : tup.getField(gbfield);
    	
    	if (null != key) {
    		if (cnts.containsKey(key)) {
	    		cnts.put(key, cnts.get(key)+1);
	    	} else {
	    		cnts.put(key, 1);
	    	}
	    	switch (op) {
		    	case MIN:
		    		vals.put(key, vals.containsKey(key)? 
		    				Math.min(vals.get(key), tup.getField(afield).hashCode()):tup.getField(afield).hashCode());
		    		System.out.println("min");
		    		break;
		    	case MAX:
		    		vals.put(key, vals.containsKey(key)? 
		    				Math.max(vals.get(key), tup.getField(afield).hashCode()):tup.getField(afield).hashCode());
		    		System.out.println("max");
		    		break;
		    	case COUNT:
		    		vals.put(key, cnts.get(key));
		    		System.out.println("count");
		    		break;
		    	default:
	    			vals.put(key, vals.containsKey(key)? 
		    				vals.get(key)+tup.getField(afield).hashCode():tup.getField(afield).hashCode());
	    			System.out.println(op);
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
				// Done
                child = vals.keySet().iterator();
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
				int val = (op == Op.AVG)? vals.get(key)/cnts.get(key) : vals.get(key);
				
				if (1 == desc.numFields()) {
					tup.setField(0, new IntField(val));
				} else {
					tup.setField(0, key);
					tup.setField(1, new IntField(val));
				}
				return tup;
			}
			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				child = vals.keySet().iterator();
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

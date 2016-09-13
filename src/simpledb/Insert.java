package simpledb;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {

	private final TransactionId tid;
	private final DbIterator child;
	private final DbFile dbfile;
	private boolean state;
	
    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId tid, DbIterator child, int tableid)
        throws DbException {
    	// Done
    	this.tid = tid;
    	this.child = child;
    	dbfile = Database.getCatalog().getDatabaseFile(tableid);
    }

    public TupleDesc getTupleDesc() {
        // Done
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // Done
    	child.open();
    }

    public void close() {
        // Done
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // Done
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
    	// Done
	    if (state) {
	    	return null;
	    } else {
	    	Tuple ret = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
	    	int num = 0;
	    	
	    	while (child.hasNext()) {
	    		try {
	    			Database.getBufferPool().insertTuple(tid, dbfile.getId(), child.next());
				} catch (Exception e) {
					throw new DbException("insertTuple failed");
				}
	    		num++;
	    	}
	    	ret.setField(0, new IntField(num));
	    	state = true;
	        return ret;
	    }
    }
}

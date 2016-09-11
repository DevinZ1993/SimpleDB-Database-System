package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

	private final TransactionId tid;
	private final DbIterator child;
	private boolean state;
	
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	// Done
    	tid = t;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // Done
        return child.getTupleDesc();
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // Done
    	if (state) {
    		return null;
    	} else {
	    	Tuple ret = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
	    	BufferPool pool = Database.getBufferPool();
	    	int num = 0;
	    	
	    	while (child.hasNext()) {
	    		pool.deleteTuple(tid, child.next());
	    		num++;
	    	}
	    	ret.setField(0, new IntField(num));
	    	state = true;
	        return ret;
    	}
    }
}

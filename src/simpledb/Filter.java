package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {

	private final Predicate predicate;
	private final DbIterator child;
	
    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // Done
    	predicate = p;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // Done
    	return child.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
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
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // Done
    	while (child.hasNext()) {
    		Tuple tup = child.next();
    		
    		if (predicate.filter(tup)) {
    			return tup;
    		}
    	}
    	return null;
    }
}

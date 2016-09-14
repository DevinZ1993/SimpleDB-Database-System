package simpledb;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

	public static TransactionId of(long id) {	// newly-defined
		return new TransactionId(id);
	}
	
    private static final long serialVersionUID = 1L;

    static AtomicLong counter = new AtomicLong(0);
    final long myid;

    public TransactionId() {
        myid = counter.getAndIncrement();
    }
    
    private TransactionId(long myid) {	// newly-defined
    	this.myid = myid;
    }

    public long getId() {
        return myid;
    }

    public boolean equals(Object o) {	// revised
    	if (!(o instanceof TransactionId)) {
    		return false;
    	} else {
    		return ((TransactionId) o).myid == myid;
    	}
    }

    public int hashCode() {
        return (int) myid;
    }
}

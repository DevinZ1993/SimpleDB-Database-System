package simpledb;

import java.io.*;
import java.util.List;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private final Page[] buffer;
    public final BufferLock lock;
    private int evictIdx = 0;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // Done
    	buffer = new Page[numPages];
    	lock = new BufferLock(numPages);
    }
    
    public static int getPageSize() {
    	return PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	// Done
    	int idx = -1;
    	
    	for (int i=0; i<buffer.length; i++) {
    		if (null == buffer[i]) {
    			idx = i;
    		} else if (pid.equals(buffer[i].getId())) {
    			try {
    				lock.getLock(tid, i, perm);
    			} catch (InterruptedException e) {
    				throw new TransactionAbortedException();
    			}
    			return buffer[i];
    		}
    	}
    	if (idx < 0) {
    		evictPage();
    		return getPage(tid, pid, perm);
    	} else {
    		try {
    			lock.getLock(tid, idx, perm);
    		} catch (InterruptedException e) {
    			throw new TransactionAbortedException();
    		}
    		return buffer[idx] = Database.getCatalog().getDatabaseFile
					(pid.getTableId()).readPage(pid);
    	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // Done
    	for (int i=0; i<buffer.length; i++) {
    		if (null != buffer[i] && buffer[i].getId().equals(pid)) {
    			if (lock.holdsLock(tid, i)) {
    				lock.releaseLock(tid, i);
    				return;
    			}
    		}
    	}
    	throw new IllegalArgumentException("page not in buffer");
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // Done
    	for (int i=0; i<buffer.length; i++) {
    		if (null != buffer[i] && buffer[i].getId().equals(pid)) {
    			return lock.holdsLock(tid, i);
    		}
    	}
        return false;
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // Done
    	transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // Done
    	if (commit) {
    		flushPages(tid);
    	}
    	for (int i=0; i<buffer.length; i++) {
    		if (lock.holdsLock(tid, i)) {
    			if (!commit && null != buffer[i] &&
    					tid.equals(buffer[i].isDirty())) {
    				buffer[i] = null;
    			}
    			lock.releaseLock(tid, i);
    		}
    	}
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // Done
    	List<Page> list = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    	
    	for (Page page : list) {
    		page.markDirty(true, tid);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // Done
    	Database.getCatalog().getDatabaseFile
    		(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t).markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // Done
    	for (int i=0; i<buffer.length; i++) {
    		if (null != buffer[i]) {
    			flushPage(buffer[i].getId());
    		}
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // Done
    	for (int i=0; i<buffer.length; i++) {
    		if (null != buffer[i] && pid.equals(buffer[i].getId())) {
    			buffer[i] = null;
    			break;
    		}
    	}
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // Done
    	for (int i=0; i<buffer.length; i++) {
    		if (null != buffer[i] && buffer[i].getId().equals(pid)) {
    			TransactionId dirtier = buffer[i].isDirty();
    			
    			if (null != dirtier) {
    				Database.getLogFile().logWrite(dirtier, buffer[i].getBeforeImage(), buffer[i]);
    				Database.getLogFile().force();
    				Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(buffer[i]);
    				buffer[i].markDirty(false, buffer[i].isDirty());
    			}
    			break;
    		}
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // Done
    	for (int i=0; i<buffer.length; i++) {
    		if (null != buffer[i] && lock.holdsLock(tid, i)) {
    			flushPage(buffer[i].getId());
    			buffer[i].setBeforeImage();
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // Done
    	for (int init=evictIdx; null!=buffer[evictIdx] && 
    			null!=buffer[evictIdx].isDirty(); ) {
    		evictIdx = (evictIdx+1)%buffer.length;
    		if (init == evictIdx) {
    			throw new DbException("no non-dirty page to evict");
    		}
    	}
    	if (null != buffer[evictIdx]) {
    		try {
				flushPage(buffer[evictIdx].getId());
				buffer[evictIdx] = null;
				evictIdx = (evictIdx+1)%buffer.length;
			} catch (IOException e) {
				throw new DbException(e.getMessage());
			}
    	}
    }

}

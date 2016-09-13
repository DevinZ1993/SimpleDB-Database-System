package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private final File file;
	private final TupleDesc td;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param file
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File file, TupleDesc td) {
        // Done
    	this.file = file;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // Done
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // Done
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // Done
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	// Done
    	if (getId() == pid.getTableId()) {
    		int pgNo = pid.pageNumber();
    		
    		if (pgNo>=0 && pgNo<numPages()) {
    			byte[] bytes = HeapPage.createEmptyPageData();
	    			
    			try {
    				RandomAccessFile raf = new RandomAccessFile(file, "r");
    				
    				try {
    					raf.seek(1L*BufferPool.getPageSize()*pid.pageNumber());
						raf.read(bytes, 0, BufferPool.getPageSize());
						return new HeapPage(HeapPageId.valueOf(pid), bytes);
    				} finally {
    					raf.close();
    				}
    			} catch (IOException e) {
    				throw new RuntimeException(e);
    			}
    		}
    	}
        throw new IllegalArgumentException("page not in the file");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // Done
		int pgNo = page.getId().pageNumber();
		
		if (pgNo>=0 && pgNo<=numPages()) {
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			
			try {
				raf.seek(1L*BufferPool.getPageSize()*pgNo);
				raf.write(page.getPageData(), 0, BufferPool.getPageSize());
				return;
			} finally {
				raf.close();
			}
		}
		throw new IllegalArgumentException("pageId out of range");
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // Done
        return (int)(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // Done
    	ArrayList<Page> list = new ArrayList<Page>();
    	BufferPool pool = Database.getBufferPool();
    	int tableId = getId(), pgNo = 0;
    	
    	for (; pgNo<numPages(); pgNo++) {
    		HeapPage page = (HeapPage)pool.getPage(tid, new HeapPageId(tableId,pgNo), Permissions.READ_WRITE);
    		
    		if (page.getNumEmptySlots() > 0) {
    			page.insertTuple(t);
    			list.add(page);
    			break;
    		}
    	}
    	if (pgNo == numPages()) {
    		HeapPage page = new HeapPage(new HeapPageId(tableId, pgNo), HeapPage.createEmptyPageData());
    		
    		page.insertTuple(t);
    		list.add(page);
    		writePage(page);
    	}
        return list;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // Done
    	BufferPool pool = Database.getBufferPool();
    	HeapPage page = (HeapPage)pool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	
    	page.deleteTuple(t);
        return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(final TransactionId tid) {
        // Done
        return new DbFileIterator() {

        	private final BufferPool pool = Database.getBufferPool();
        	private final int tableId = getId();
        	private int pid = -1;
        	private Iterator<Tuple> child;
        	
			@Override
			public void open() throws DbException, TransactionAbortedException {
				pid = 0;
				child = null;
			}
			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				if (null != child && child.hasNext()) {
					return true;
				} else if (pid < 0 || pid >= numPages()) {
					return false;
				} else {
					child = ((HeapPage)pool.getPage(tid, new HeapPageId(tableId,pid++), 
							Permissions.READ_ONLY)).iterator();
					return hasNext();
				}
			}
			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				if (!hasNext()) {
					throw new NoSuchElementException();
				} else {
					return child.next();
				}
			}
			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				pid = 0;
				child = null;
			}
			@Override
			public void close() {
				pid = -1;
				child = null;
			}
        };
    }

}


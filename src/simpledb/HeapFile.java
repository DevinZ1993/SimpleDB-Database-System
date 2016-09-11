package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private final int tableId;
	private final File file;
	private final TupleDesc desc;
	
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // Done
    	tableId = f.getAbsoluteFile().hashCode();
    	file = f;
    	desc = td;
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
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        // Done
    	return tableId;
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	// Done
    	return desc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // Done
    	if (getId() == pid.getTableId()) {
    		int pageno = pid.pageno();
    		
    		if (pageno>=0 && pageno<=numPages()) {
    			byte[] bytes = HeapPage.createEmptyPageData();
	    			
    			try {
    				if (pageno < numPages()) {
	    				RandomAccessFile raf = new RandomAccessFile(file, "r");
	    				raf.seek(1L*BufferPool.PAGE_SIZE*pid.pageno());
						raf.read(bytes, 0, BufferPool.PAGE_SIZE);
						raf.close();
	    			}
    				return new HeapPage(new HeapPageId
			    			(pid.getTableId(), pid.pageno()), bytes);
    			} catch (IOException e) {
    				e.printStackTrace();
    				return null;
    			}
    		}
    	}
        throw new IllegalArgumentException("page not in the file");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // Done
    	PageId pid = page.getId();
    	
    	if (getId() == pid.getTableId()) {
    		int pageno = pid.pageno();
    		
    		if (pageno>=0 && pageno<=numPages()) {
    			try {
					RandomAccessFile raf = new RandomAccessFile(file, "rw");
					
					try {
						raf.seek(1L*BufferPool.PAGE_SIZE*pid.pageno());
						raf.write(page.getPageData(), 0, BufferPool.PAGE_SIZE);
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						raf.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
    		}
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // Done
        return (int)file.length()/BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // Done
    	ArrayList<Page> list = new ArrayList<Page>();
    	HeapPageId pageId = new HeapPageId(tableId, Math.max(0, numPages()-1));
    	BufferPool pool = Database.getBufferPool();
    	HeapPage page = (HeapPage)pool.getPage(tid, pageId, Permissions.READ_WRITE);
    	
    	if (0 == page.getNumEmptySlots()) {
    		pageId = new HeapPageId(tableId, numPages());
    		page = (HeapPage)pool.getPage(tid, pageId, Permissions.READ_WRITE);
    	}
    	page.addTuple(t);
    	writePage(page);
    	list.add(page);
    	return list;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // Done
    	BufferPool pool = Database.getBufferPool();
    	HeapPage page = (HeapPage)pool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	
    	
    	page.deleteTuple(t);
    	try {
			writePage(page);
		} catch (IOException e) {
			throw new DbException("writePage failed");
		}
        return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(final TransactionId tid) {
        // Done
        return new DbFileIterator() {
        	
        	private int pageno = 0;
        	private HeapPage page;
        	private Iterator<Tuple> iterator;
        	
			@Override
			public void open() throws DbException, TransactionAbortedException {
				page = (HeapPage)Database.getBufferPool().getPage
						(tid, new HeapPageId(tableId, pageno++), Permissions.READ_ONLY);
				iterator = page.iterator();
			}
			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				if (null == iterator) {
					return false;
				} else if (iterator.hasNext()) {
					return true;
				} else if (pageno >= numPages()) {
					return false;
				} else {
					page = (HeapPage)Database.getBufferPool().getPage
							(tid, new HeapPageId(tableId, pageno++), Permissions.READ_ONLY);
					iterator = page.iterator();
					return hasNext();
				}
			}
			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				if (!hasNext()) {
					throw new NoSuchElementException();
				} else if (iterator.hasNext()) {
					return iterator.next();
				} else {
					page = (HeapPage)Database.getBufferPool().getPage
							(tid, new HeapPageId(tableId, pageno++), Permissions.READ_ONLY);
					iterator = page.iterator();
					return iterator.next();
				}
			}
			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				close();
				open();
			}
			@Override
			public void close() {
				pageno = 0;
				page = null;
				iterator = null;
			}
        };
    }
    
}


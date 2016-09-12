package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
	
	public static HeapPageId valueOf(PageId pageId) {	/* newly-defined */
    	return new HeapPageId(pageId.getTableId(), pageId.pageNumber());
    }
    
	private final int tableId, pgNo;
	
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // Done
    	this.tableId = tableId;
    	this.pgNo = pgNo;
    }
    
    /** @return the table associated with this PageId */
    public int getTableId() {
        // Done
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        // Done
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // Done
    	return 31*tableId+pgNo;
    }

    @Override
    public String toString() {	// newly-defined
    	return "Page("+tableId+","+pgNo+")";
    }
    
    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
    	// Done
    	if (!(o instanceof PageId)) {
    		return false;
    	} else {
    		PageId other = (PageId)o;
    		
    		return getTableId() == other.getTableId() &&
    				pageNumber() == other.pageNumber();
    	}
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}

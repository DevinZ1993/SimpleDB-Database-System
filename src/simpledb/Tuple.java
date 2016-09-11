package simpledb;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {

	private final TupleDesc desc;
	private final Field[] fields;
	private RecordId rid;
	
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // Done
    	desc = td;
    	fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
    	// Done
        return desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
    	// Done
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // Done
    	this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // Done
    	if (i>=0 && i<fields.length && desc.getType(i) == f.getType()) {
    		fields[i] = f;
    	}
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // Done
        if (i<0 || i>=fields.length) {
        	return null;
        } else {
        	return fields[i];
        }
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // Done
    	StringBuilder sb = new StringBuilder();
    	
    	sb.append(fields[0].toString());
    	for  (int i=1; i<fields.length; i++) {
    		sb.append("\t"+fields[i].toString());
    	}
    	//sb.append('\n');
        return sb.toString();
    }
}

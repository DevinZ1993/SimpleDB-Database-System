package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final Type[] types;
    private final String[] names;
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // Done
        return new Iterator<TDItem>() {
        	private int idx = -1;
        	
			@Override
			public boolean hasNext() {
				return idx+1<types.length;
			}
			@Override
			public TDItem next() {
				if (++idx == types.length) {
					throw new NoSuchElementException();
				} else {
					return new TDItem(types[idx], names[idx]);
				}
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException("unimplemented");
			}
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // Done
    	types = new Type[typeAr.length];
    	for (int i=0; i<typeAr.length; i++) {
    		types[i] = typeAr[i];
    	}
    	names = new String[typeAr.length];
    	for (int i=0; i<fieldAr.length; i++) {
    		names[i] = fieldAr[i];
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // Done
    	this(typeAr, new String[0]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // Done
    	return types.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // Done
    	if (i<0 || i>=types.length) {
    		throw new NoSuchElementException("Index Out of Range.");
    	} else {
    		return names[i];
    	}
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // Done
    	if (i<0 || i>=types.length) {
    		throw new NoSuchElementException("Index Out of Range.");
    	} else {
    		return types[i];
    	}
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // Done
    	if (null != name) {
    		for (int i=0; i<names.length; i++) {
    			if (name.equals(names[i])) {
    				return i;
    			}
    		}
    	}
    	throw new NoSuchElementException("Unknown Field Name.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // Done
    	int size = 0;
    	
    	for (int i=0; i<types.length; i++) {
    		size += types[i].getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // Done
    	Type[] types = new Type[td1.types.length+td2.types.length];
    	String[] names = new String[td1.names.length+td2.names.length];
    	int idx = 0;
    	
    	for (int i=0; i<td1.types.length; i++) {
    		types[idx] = td1.types[i];
    		names[idx++] = td1.names[i];
    	}
    	for (int i=0; i<td2.types.length; i++) {
    		types[idx] = td2.types[i];
    		names[idx++] = td2.names[i];
    	}
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	// Done
        if (!(o instanceof TupleDesc)) {
        	return false;
        } else {
        	TupleDesc other = (TupleDesc)o;
        	
        	if (this.numFields() != other.numFields()) {
        		return false;
        	} else {
        		int n = this.numFields();
        		
        		for (int i=0; i<n; i++) {
        			if (null == this.getFieldName(i)) {
        				if (null != other.getFieldName(i)) {
        					return false;
        				}
        			} else if (this.getFieldName(i).equals(other.getFieldName(i))) {
        				return false;
        			} else if (this.getFieldType(i) != other.getFieldType(i)) { // Will this work ?
        				return false;
        			}
        		}
        		return true;
        	}
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	// Done
    	StringBuilder sb = new StringBuilder();
    	int n = this.numFields();
    	
    	sb.append(getFieldType(0));
    	sb.append("("+this.getFieldName(0)+")");
    	for (int i=1; i<n; i++) {
    		sb.append(","+getFieldType(i));
        	sb.append("("+this.getFieldName(i)+")");
    	}
        return sb.toString();
    }
}

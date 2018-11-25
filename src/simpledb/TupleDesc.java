package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
    //private Map<String, Type>attributes;
    private Type[] attrTypes;
    private String[] attrNames;
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        if (td1 == null || td2 == null) {
            return null;
        }
        int numFields1 = td1.numFields();
        int numFields2 = td2.numFields();
        int newNumFields = numFields1 + numFields2;
        Type[] newAttrTypes = new Type[newNumFields];
        String[] newAttrNames = new String[newNumFields];
        for (int index1 = 0; index1 < numFields1; ++index1) {
            newAttrTypes[index1] = td1.getType(index1);
            newAttrNames[index1] = td1.getFieldName(index1);
        }
        for (int index2 = 0; index2 < numFields2; ++index2) {
            newAttrTypes[numFields1 + index2] = td2.getType(index2);
            newAttrNames[numFields1 + index2] = td2.getFieldName(index2);
        }
        return new TupleDesc(newAttrTypes, newAttrNames);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        attrTypes = typeAr;
        attrNames = fieldAr;
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        attrTypes = typeAr;
        attrNames = new String[typeAr.length];
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return attrTypes.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i > numFields()) {
            throw new NoSuchElementException();
        }
        return i < attrNames.length? attrNames[i] : null;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < numFields(); ++i) {
            if (name.equals(getFieldName(i))) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        if (i < 0 || i > numFields()) {
            throw new NoSuchElementException();
        }
        return attrTypes[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(Type t : attrTypes) {
            size += t.getLen();
        }
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc)o;
        if (other.numFields() != numFields()) {
            return false;
        }
        for (int i = 0; i < numFields(); ++i) {
            if (other.getType(i) != getType(i)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
//        throw new UnsupportedOperationException("unimplemented");
        int h = numFields();
        for (int i = 0; i < numFields(); ++i) {
            h += getType(i) == Type.INT_TYPE ? 1 : 2;
        }
        return h;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "attrTypes[0](attrNames[0]), ..., attrTypes[M](attrNames[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        String formatted = "";
        for (int i = 0; i < numFields(); ++i) {
            if (i != 0) {
                formatted = formatted.concat(", ");
            }
            formatted = formatted.concat(getType(i) == Type.INT_TYPE ? "Int" : "String");
            if (getFieldName(i) != null) {
                formatted = formatted.concat("(" + getFieldName(i) + ")");
            }
        }
        return formatted;
    }
}

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
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    public ArrayList<TDItem> tdlist;

    /*
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdlist.iterator();
    }

    private static final long serialVersionUID = 1L;

    /* *
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
        
        tdlist = new ArrayList<TDItem>(typeAr.length);
        for(int i=0;i<typeAr.length;i++)
            tdlist.add(new TDItem(typeAr[i],fieldAr[i]));


        // some code goes here
    }

    /* *
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
       
         tdlist = new ArrayList<TDItem>(typeAr.length);
        for(int j=0; j<typeAr.length;j++)
            tdlist.add(new TDItem(typeAr[j],null));


        // some code goes here
    }

    /* *
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdlist.size();
    }

    /* *
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0 || i>numFields())
            throw new NoSuchElementException();
        return tdlist.get(i).fieldName;
    }

    /* *
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
         // some code goes here
        if(i<0 || i>numFields())
            throw new NoSuchElementException();
        return tdlist.get(i).fieldType;
       
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
        // some code goes here
        int num;
        boolean numval=true;
        if(name==null)
            throw new NoSuchElementException("not found");
        
        for(int i=0;i<tdlist.size();i++)
        {

            String nm= tdlist.get(i).fieldName;
            if(nm==null) continue;
            numval=false;

            if(nm.equals(name))
                return i;
    
        }
        if(numval)
            throw new NoSuchElementException("All empty");
        throw new NoSuchElementException("Name doesnt exist");

       
    }

    /* *
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int lentdlist=0;
        Type m;
        for(int j=0;j<tdlist.size();j++){
            m=tdlist.get(j).fieldType;

            lentdlist=lentdlist + m.getLen();
        }
        // some code goes here
        return lentdlist;
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
        
        int news=td1.numFields()+td2.numFields();
        Type[] newfieldtype = new Type[news];
        String[] newfieldname = new String[news];
        for(int i=0;i<td1.numFields();i++){
            newfieldname[i]=td1.getFieldName(i);
            newfieldtype[i]=td1.getFieldType(i);

        }
        for(int i=0;i<td2.numFields();i++){
            newfieldname[i+td1.numFields()]=td2.getFieldName(i);
            newfieldtype[i+td1.numFields()]=td2.getFieldType(i);

        }


        // some code goes here
        return new TupleDesc(newfieldtype,newfieldname);
    }

    /* *
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {

                if(o==null)
                    return false;
                if(!(o instanceof TupleDesc))
                    return false;
               TupleDesc otd = (TupleDesc) o;
        int flag,count,i;
        flag=0;
        count=0;

        if(this.numFields()==otd.numFields() && this.getSize()==otd.getSize()){
            flag=1;
            for(i=0;i<this.numFields();i++)
            {
                if(this.getFieldType(i).equals(otd.getFieldType(i)))
                                    count++;
            }

        }

        if(count==this.numFields() && flag==1)
        // some code goes here
        return true;
        return false;
    }


    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /* *
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String tupldesc = null;
       
        for(int i=0;i<this.numFields();i++){

            tupldesc = tupldesc + this.getFieldType(i)+"["+Integer.toString(i)+"]("+this.getFieldName(i)+"["+Integer.toString(i)+"]),";
        }
        // some code goes here
        return tupldesc;
    }
}
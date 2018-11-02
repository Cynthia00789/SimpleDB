package simpledb;

import java.io.IOException;
/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private TupleDesc td;

    // Below variable is true when fetchNext() has been called
    boolean insert_called = false;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableid = tableid;
        //if(!(child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))))
        //    throw DbException("TupleDesc of child doesn't match the table's tupdesc");

        Type[] typear = {Type.INT_TYPE};
        String[] fieldn = {"No of tuples inserted"};
        td = new TupleDesc(typear,fieldn); 

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(insert_called)
            return null;  
        insert_called = true; 
        int i = 0; //no of tuples inserted
        try{
            BufferPool bp = Database.getBufferPool();
            while(child.hasNext()){
                Tuple tp = child.next();
                try{
                    bp.insertTuple(t,tableid,tp);
                }catch(IOException e){
                    e.printStackTrace();
                }
                i++;
            }
            
        }catch(DbException e){
            e.printStackTrace();
        }
        Tuple resultTup = new Tuple(td);
        resultTup.setField(0, new IntField(i));
        return resultTup;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }
}


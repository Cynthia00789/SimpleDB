package simpledb;

import java.io.IOException;
/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private TupleDesc td;

    // Below variable is true when fetchNext() has been called
    boolean fetch_called = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
        Type[] typear = new Type[]{Type.INT_TYPE};
        String[] fieldn = new String[]{"No of tuples deleted"};
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(fetch_called)
            return null;
        fetch_called = true;
        int i = 0; //no of tuples inserted
        try{
            BufferPool bp = Database.getBufferPool();
            while(child.hasNext()){
                Tuple tp = child.next();
                bp.deleteTuple(t,tp);
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
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }

}

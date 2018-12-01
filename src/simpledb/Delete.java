package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
    private DbIterator child;
    private TransactionId tid;
    private TupleDesc td;
    private boolean isVisited;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.isVisited = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
    }

    public void close() {
        // some code goes here
        child.close();
        isVisited = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (isVisited) {
            return null;
        }
        int deletedNum = 0;
        while (child.hasNext()) {
            Database.getBufferPool().deleteTuple(tid, child.next());
            ++deletedNum;
        }
        Tuple result = new Tuple(td);
        result.setField(0, new IntField(deletedNum));
        isVisited = true;
        return result;
    }
}

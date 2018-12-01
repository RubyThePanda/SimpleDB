package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
    private DbIterator child;
    private TransactionId tid;
    private int tableId;
    private TupleDesc resTD;
    private boolean isVisited;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.child = child;
        this.tid = t;
        this.tableId = tableid;
        resTD = new TupleDesc(new Type[]{Type.INT_TYPE});
        isVisited = false;
    }

    public TupleDesc getTupleDesc() {
        return resTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    }

    public void close() {
        child.close();
        isVisited = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        if (isVisited) { return null;}
        int insertedNum = 0;
        while(child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
                ++insertedNum;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple result = new Tuple(resTD);
        result.setField(0, new IntField(insertedNum));
        isVisited = true;
        return result;
    }
}

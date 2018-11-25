package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
    private TransactionId tid;
    private int tableId;
    private String tableAlias;
    private TupleDesc td;
    private int numPages;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;

        TupleDesc td = Database.getCatalog().getTupleDesc(tableId);
        int numFields = td.numFields();
        Type[] types = new Type[numFields];
        String[] fieldNames = new String[numFields];
        String prefix = (tableAlias == null) ? "null" : tableAlias;
        for (int i = 0; i < numFields; ++i) {
            types[i] = td.getType(i);
            String fieldName = (td.getFieldName(i) == null) ? "null" : td.getFieldName(i);
            fieldNames[i] = tableAlias + "." + fieldName;
        }
        this.td = new TupleDesc(types, fieldNames);

        DbFile dbFile = Database.getCatalog().getDbFile(tableid);
        if (dbFile instanceof HeapFile) {
            HeapFile heapFile = (HeapFile)dbFile;
            numPages = heapFile.numPages();
        } else {
            numPages = 0;
        }
    }

    private boolean isOpen = false;
    private int currentPageNo = 0;
    private Iterator<Tuple> currentTupleIterator;

    private void setCurrentTupleIterator() throws DbException, TransactionAbortedException {
        Page currentPage = Database.getBufferPool().getPage(tid, new HeapPageId(tableId, currentPageNo), Permissions.READ_ONLY);
        if (currentPage instanceof HeapPage) {
            HeapPage heapPage = (HeapPage)currentPage;
            currentTupleIterator = heapPage.iterator();
        }
    }
    public void open()
        throws DbException, TransactionAbortedException {
        isOpen = true;
        setCurrentTupleIterator();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (!isOpen || currentPageNo >= numPages) {
            return false;
        } else {
            if (currentTupleIterator.hasNext()) {
                return true;
            } else if (currentPageNo != numPages - 1) {
                ++currentPageNo;
                setCurrentTupleIterator();
                return hasNext();
            }
        }
        return false;
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        if (!isOpen) {
            throw new NoSuchElementException();
        } else {
            return currentTupleIterator.next();
        }
    }

    public void close() {
        isOpen = false;
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        currentPageNo = 0;
        setCurrentTupleIterator();
    }
}

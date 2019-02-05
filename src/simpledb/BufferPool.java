package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private ConcurrentMap<PageId, Page> bufferedPages;
    private LockTable lockTable;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        bufferedPages = new ConcurrentHashMap<PageId, Page>();
        lockTable = new LockTable();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        try {
            lockTable.acquireLock(pid, tid, perm);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (bufferedPages.containsKey(pid)) {
            return bufferedPages.get(pid);
        } else {
            int tableId = pid.getTableId();
            DbFile dbFile = Database.getCatalog().getDbFile(tableId);
            Page page = dbFile.readPage(pid);
            if (bufferedPages.size() >= numPages) {
                evictPage();
            }
            bufferedPages.put(pid, page);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockTable.releaseLocks(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId p) {
        return lockTable.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if (commit) {
            // When you commit, you should flush dirty pages associated to the transaction to disk.
            // Done in Transaction::transactionComplete()
        } else {
            // When you abort, you should revert any changes made by the transaction by restoring the page to its on-disk state.
            evictDirtyPages(tid);
        }

        // Whether the transaction commits or aborts,
        // you should also release any state the BufferPool keeps regarding the transaction,
        // including releasing any locks that the transaction held.
        releaseLocks(tid);
    }

    private void releaseLocks(TransactionId tid) {
            lockTable.releaseLocks(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile heapFile = (HeapFile)Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> updatedPages = heapFile.addTuple(tid, t);
        for (Page page : updatedPages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        HeapFile heapFile = (HeapFile)Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
        Page updatedPage = heapFile.deleteTuple(tid, t);
        updatedPage.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId : bufferedPages.keySet()) {
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = bufferedPages.get(pid);
        DbFile dbFile = Database.getCatalog().getDbFile(page.getId().getTableId());
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : bufferedPages.keySet()) {
            TransactionId dirtyTid = bufferedPages.get(pid).isDirty();
            if (dirtyTid != null && dirtyTid.equals(tid)) {
                flushPage(pid);
            }
        }
    }

    private  synchronized void evictDirtyPages(TransactionId tid) {
        for (PageId pid : bufferedPages.keySet()) {
            if (bufferedPages.get(pid).isDirty() != null && bufferedPages.get(pid).isDirty().equals(tid)) {
                bufferedPages.remove(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        /*
        Modifications from a transaction are written to disk only after it commits.
        This means we can abort a transaction by discarding the dirty pages and rereading them from disk.
        Thus, we must not evict dirty pages.
        This policy is called NO STEAL.
         */
        if (bufferedPages.isEmpty()) {
            return;
        }

        for (PageId pageId : bufferedPages.keySet()) {
            if (bufferedPages.get(pageId).isDirty() == null) {
                try {
                    flushPage(pageId);
                    bufferedPages.remove(pageId);
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        throw new DbException("Buffered pages may be all dirty");
    }
}

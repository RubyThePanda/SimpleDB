package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private TupleDesc tupleDesc;
    private File dbFile;
    private int fileId;
    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        dbFile = f;
        tupleDesc = td;
        fileId = f.getAbsoluteFile().hashCode();
        numPages = (int) Math.ceil(f.length() / (double)BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return dbFile;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return fileId;
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (pid instanceof HeapPageId) {
            HeapPageId heapPid = (HeapPageId)pid;
            int offset = BufferPool.PAGE_SIZE * pid.pageno();
            try {
                RandomAccessFile raf = new RandomAccessFile(dbFile, "r");
                raf.seek(offset);
                byte[] bytes = new byte[BufferPool.PAGE_SIZE];
                raf.read(bytes);
                raf.close();
                return new HeapPage(heapPid, bytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // TODO:
            return null;
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int offset = BufferPool.PAGE_SIZE * page.getId().pageno();
        RandomAccessFile raf = new RandomAccessFile(dbFile, "rw");
        raf.seek(offset);
        byte[] data = page.getPageData();
        raf.write(data);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        Page insertedPage = null;
        for (int i = 0; i < numPages; ++i) {
            HeapPageId pid = new HeapPageId(fileId, i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() > 0) {
                heapPage.addTuple(t);
                insertedPage = heapPage;
                break;
            }
        }
        if (insertedPage == null) {
            HeapPageId pid = new HeapPageId(fileId, numPages);
            HeapPage newHeapPage = new HeapPage(pid, HeapPage.createEmptyPageData());
            writePage(newHeapPage);
            ++numPages;
            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            heapPage.addTuple(t);
            insertedPage = heapPage;
        }
        ArrayList<Page> result = new ArrayList<Page>(1);
        result.add(insertedPage);
        return result;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        if (page instanceof HeapPage) {
            HeapPage heapPage = (HeapPage)page;
            heapPage.deleteTuple(t);
        }
        return page;
    }

    private class HeapFileIterator implements DbFileIterator {
        private int pageCursor;
        private Iterator<Tuple> currentTupleIterator;
        private boolean isOpen = false;
        private TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        private void setCurrentTupleIterator() throws TransactionAbortedException, DbException {
            if (pageCursor < 0 || pageCursor >= numPages()) {
                return;
            }
            Page currentPage = Database.getBufferPool().getPage(tid, new HeapPageId(fileId, pageCursor), Permissions.READ_ONLY);
            if (currentPage instanceof HeapPage) {
                currentTupleIterator = ((HeapPage)currentPage).iterator();
            } else {
                // TODO:
            }
        }
        public void open() throws DbException, TransactionAbortedException {
            pageCursor = 0;
            setCurrentTupleIterator();
            isOpen = true;
        }

        public void close(){
            isOpen = false;
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen || pageCursor < 0 || pageCursor >= numPages()) {
                System.out.println("End");
                return false;
            } else {
                if (currentTupleIterator.hasNext()) {
                    return true;
                } else if (pageCursor != numPages() - 1){
                    ++pageCursor;
                    setCurrentTupleIterator();
                    return hasNext();
                }
            }
            return false;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new NoSuchElementException();
            }
            return currentTupleIterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            pageCursor = 0;
            setCurrentTupleIterator();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid)
    {
        return new HeapFileIterator(tid);
    }
    
}


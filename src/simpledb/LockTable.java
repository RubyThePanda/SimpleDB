package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockTable {
    ConcurrentMap<PageId, LockStatus> lockTable;

    public LockTable() {
        lockTable = new ConcurrentHashMap<PageId, LockStatus>();
    }

    synchronized public void  acquireLock(PageId pid, TransactionId tid, Permissions perm) throws InterruptedException {
        LockStatus lockStatus = lockTable.get(pid);
        if (lockStatus == null) {
            lockTable.put(pid, new LockStatus(tid, perm));
        } else {
            while(lockStatus.addLock(tid, perm) == false) {
                Thread.sleep(1000);
            }
        }
    }

    synchronized  public void releaseLocks(TransactionId tid) {
        for (LockStatus lockStatus : lockTable.values()) {
            lockStatus.releaseLock(tid);
        }
    }

    synchronized  public void releaseLocks(PageId pid, TransactionId tid) {
        LockStatus lockStatus = lockTable.get(pid);
        if (lockStatus != null) {
            lockStatus.releaseLock(tid);
        }
    }

    synchronized public boolean holdsLock(PageId pid, TransactionId tid) {
        LockStatus lockStatus = lockTable.get(pid);
        if (lockStatus != null) {
            return lockStatus.holdsLock(tid);
        }
        return false;
    }
}

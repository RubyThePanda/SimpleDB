package simpledb;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockTable {
    ConcurrentMap<PageId, LockStatus> lockTable;
    DeadlockDetector deadlockDetector;

    public LockTable() {
        lockTable = new ConcurrentHashMap<PageId, LockStatus>();
        deadlockDetector = new DeadlockDetector();
    }

     public void  acquireLock(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException {
        LockStatus lockStatus = lockTable.get(pid);
        if (lockStatus == null) {
//            System.out.println("Add new LockStatus: " + tid.toString() + " acquire " + perm.toString() + " lock on " + pid.toString());
            lockTable.put(pid, new LockStatus(tid, perm));
        } else {
            while(true) {
//                System.out.println("Try to acquire lock: " + tid.toString() + " ask " + perm.toString() + " lock on " + pid.toString());
                Set<TransactionId> dependencies = lockStatus.addLock(tid, perm);
                if (dependencies.isEmpty()) {
                    break;
                }
                if (deadlockDetector.detectDeadLock(tid, dependencies)) {
                    throw new TransactionAbortedException();
                } else {
                    deadlockDetector.addDependency(tid, dependencies);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    synchronized  public void releaseLocks(TransactionId tid) {
        for (LockStatus lockStatus : lockTable.values()) {
            lockStatus.releaseLock(tid);
        }
        deadlockDetector.removeDependency(tid);
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

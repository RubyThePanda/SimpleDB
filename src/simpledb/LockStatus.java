package simpledb;
import java.util.HashSet;
import java.util.Set;

public class LockStatus {
    private Set<TransactionId> sharedLocks;
    private TransactionId exclusiveLock;

    public LockStatus(TransactionId tid, Permissions perm) {
        init(tid, perm);
    }

    private void init() {
        sharedLocks = new HashSet<TransactionId>();
        exclusiveLock = null;
    }

    private void init(TransactionId tid, Permissions perm) {
        init();
        if (perm == Permissions.READ_WRITE) {
            exclusiveLock = tid;
        } else if (perm == Permissions.READ_ONLY) {
            sharedLocks.add(tid);
        }
    }

    /*
    Before a transaction can read an object, it must have a shared lock on it.
    Before a transaction can write an object, it must have an exclusive lock on it.
    */
    synchronized public Set<TransactionId> addLock(TransactionId transactionId, Permissions permissions) {
        Set<TransactionId> dependencies = new HashSet<TransactionId>();
        if (permissions == Permissions.READ_ONLY) {
            if (exclusiveLock == null) {
                // Multiple transactions can have a shared lock on an object.
                sharedLocks.add(transactionId);
                return dependencies;
            }
        } else if (permissions == Permissions.READ_WRITE) {
            if (exclusiveLock == null) {
                if (sharedLocks.isEmpty()) {
                    exclusiveLock = transactionId;
                } else if (sharedLocks.size() == 1 && sharedLocks.contains(transactionId)) {
                    // If transaction t is the only transaction holding a lock on an object o, t may upgrade its lock on o to a exclusive lock.
                    exclusiveLock = transactionId;
                    sharedLocks.remove(transactionId);
                } else {
                    dependencies.addAll(sharedLocks);
                }
                return dependencies;
            }
        }
        if (exclusiveLock != null && !exclusiveLock.equals(transactionId)) {
            dependencies.add(exclusiveLock);
        }

        // Only one transaction may have an exclusive lock on an object.
        // No transaction may have a shared lock on an object if another transaction has an exclusive lock on it.
        return dependencies;
    }

    synchronized public void releaseLock(TransactionId tid) {
        if (exclusiveLock != null && exclusiveLock.equals(tid)) {
            exclusiveLock = null;
        }
        sharedLocks.remove(tid);
    }

    synchronized public boolean holdsLock(TransactionId transactionId) {
        if (exclusiveLock != null && exclusiveLock.equals(transactionId)) {
            return true;
        }
        return sharedLocks.contains(transactionId);
    }
}

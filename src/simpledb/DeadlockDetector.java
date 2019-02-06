package simpledb;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DeadlockDetector {
    private ConcurrentMap<TransactionId, Set<TransactionId>> dependencies;

    public DeadlockDetector() {
        this.dependencies = new ConcurrentHashMap<TransactionId, Set<TransactionId>>();
    }

    private void printDependency(TransactionId tid, Set<TransactionId> dependentTids) {
        System.out.print("tid " + tid.toString() + " dependents on: ");
        for (TransactionId dependentTid : dependentTids) {
            System.out.print(dependentTid.toString() + " ");
        }
        System.out.println("");
    }

    public void addDependency(TransactionId tid, Set<TransactionId> dependentTids) {
        dependentTids.remove(tid);
//        System.out.print("Add dependency - ");
//        printDependency(tid, dependentTids);

        if (dependencies.containsKey(tid)) {
            dependencies.get(tid).addAll(dependentTids);
        } else {
            dependencies.put(tid, dependentTids);
        }
    }

    public void removeDependency(TransactionId tid) {
        dependencies.remove(tid);
        for (Set<TransactionId> dependentTids : dependencies.values()) {
            dependentTids.remove(tid);
        }
    }

    public boolean detectDeadLock(TransactionId newTid, Set<TransactionId> dependentTids) {
//        System.out.print("Add dependency - ");
//        printDependency(newTid, dependentTids);
        for (TransactionId dependentTid : dependentTids) {
            if (dependentTid.equals(newTid)) {
                return true;
            }
            if (dependencies.containsKey(dependentTid)) {
                if (detectDeadLock(newTid, dependencies.get(dependentTid))) {
                    return true;
                }
            }
        }
        return false;
    }
}

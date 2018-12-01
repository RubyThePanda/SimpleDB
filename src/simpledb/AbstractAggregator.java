package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public abstract class AbstractAggregator implements Aggregator{
    int gbFieldIndex; // Aggregator.NO_GROUPING: -1
    Type gbFieldType;
    int aggFieldIndex;
    Op aggOp;
    TupleDesc resTupleDesc;
    Map<Field, Double> results;

    class AggregatorIterator implements DbIterator {
        Tuple tupleResults[];
        boolean isOpen = false;
        int cursor;

        AggregatorIterator() {
            constructResults();
            cursor = 0;
            isOpen = true;
        }

        private void constructResults() {
            tupleResults = new Tuple[results.size()];
            Iterator<Map.Entry<Field, Double>> iter = results.entrySet().iterator();
            int resIndex = 0;
            while (iter.hasNext()) {
                Tuple result = new Tuple(resTupleDesc);
                Map.Entry<Field, Double> curEntry = iter.next();
                if (gbFieldIndex == Aggregator.NO_GROUPING) {
                    result.setField(0, new IntField((int)Math.floor(curEntry.getValue())));
                } else {
                    result.setField(0, curEntry.getKey());
                    result.setField(1, new IntField((int)Math.floor(curEntry.getValue())));
//                    System.out.println(curEntry.getKey().toString() + ", " + curEntry.getValue());
                }
                tupleResults[resIndex] = result;
                ++resIndex;
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (isOpen) {
                return cursor < tupleResults.length;
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen || cursor < 0 || cursor >= tupleResults.length) {
                throw new NoSuchElementException();
            }
            int curCursor = cursor;
            ++cursor;
            return tupleResults[curCursor];
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            cursor = 0;
        }

        @Override
        public TupleDesc getTupleDesc() {
            return resTupleDesc;
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }
}

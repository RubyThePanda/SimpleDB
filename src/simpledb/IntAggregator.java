package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
//public class IntAggregator implements Aggregator {
public class IntAggregator extends AbstractAggregator {

    Map<Field, Integer> counts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFieldIndex = afield;
        this.aggOp = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            resTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            resTupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        results = new HashMap<Field, Double>();
        if (what == Op.AVG) {
            counts = new HashMap<Field, Integer>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field gbField = tup.getField(gbFieldIndex); // null if gbFieldIndex == Aggregator.NO_GROUPING)
        Field aggField = tup.getField(aggFieldIndex);
        int aggVal = 0;
        if (aggField != null && aggField.getType() == Type.INT_TYPE) {
            IntField intField = (IntField)aggField;
            aggVal = intField.getValue();
        } else {
            return;
        }

        // init aggResult
        double aggResult = 0;
        if (results.containsKey(gbField)) {
            aggResult = results.get(gbField);
        } else {
            if (aggOp == Op.MIN) {
                aggResult = Integer.MAX_VALUE;
            } else if (aggOp == Op.MAX) {
                aggResult = Integer.MIN_VALUE;
            }
        }

        // init aggCount
        int aggCount = 0;
        if (counts != null && counts.containsKey(gbField)) {
            aggCount = counts.get(gbField);
            counts.put(gbField, aggCount + 1);
        } else if (counts != null){
            counts.put(gbField, aggCount + 1);
        }
        ++aggCount;

        // incremental calculate
        switch (aggOp) {
            case SUM:
                aggResult += aggVal;
                break;
            case MIN:
                aggResult = (aggResult > aggVal) ? aggVal : aggResult;
                break;
            case MAX:
                aggResult = (aggResult > aggVal) ? aggResult : aggVal;
                break;
            case COUNT:
                ++aggResult;
                break;
            case AVG:
                aggResult = aggResult + (aggVal - aggResult) / (double)aggCount;
                break;
            default:
                // TODO: error handling
                break;
        }

        results.put(gbField, aggResult);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new AggregatorIterator();
    }

}

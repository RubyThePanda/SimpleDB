package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends AbstractAggregator {

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
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
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field gbField = tup.getField(gbFieldIndex);
        Field aggField = tup.getField(aggFieldIndex);
        if (aggField == null) { return; }

        double aggResult = 0;
        if (results.containsKey(gbField)) {
            aggResult = results.get(gbField);
        }

        if (aggOp == Op.COUNT) {
            ++aggResult;
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

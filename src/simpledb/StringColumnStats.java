package simpledb;

public class StringColumnStats implements ColumnStats {
    private StringHistogram histogram;
    private int numBuckets;

    public StringColumnStats(int buckets) {
        numBuckets = buckets;
        initHistogram();
    }

    public void initHistogram() {
        histogram = new StringHistogram(numBuckets);
    }

    public void addValueToHistogram(String v) {
        histogram.addValue(v);
    }

    public double estimateSelectivity(Predicate.Op op, String v) {
        return histogram.estimateSelectivity(op, v);
    }
}

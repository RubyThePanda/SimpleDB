package simpledb;

public class IntColumnStats implements ColumnStats {
    public int min;
    public int max;
    private IntHistogram histogram;
    private boolean histogramInited;
    private int numBuckets;

    public IntColumnStats(int buckets) {
        min = Integer.MAX_VALUE;
        max = Integer.MIN_VALUE;
        numBuckets = buckets;
        histogramInited = false;
    }

    public void initHistogram() {
        histogram = new IntHistogram(numBuckets, min, max);
        histogramInited = true;
    }

    public void addValueToHistogram(int v) {
        if (!histogramInited) {
            initHistogram();
        }
        histogram.addValue(v);
    }

    public double estimateSelectivity(Predicate.Op op, int v) {
        if (!histogramInited) {
            return 1.0;
        }
        return histogram.estimateSelectivity(op, v);
    }

}

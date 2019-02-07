package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int numBuckets;
    private int minValue;
    private int maxValue;
    private int width;
    private int[] histogram;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        numBuckets = Math.min(buckets, max - min + 1);
        minValue = min;
        maxValue = max;
        width = (maxValue - minValue + 1) / numBuckets;
        histogram = new int[numBuckets];
        ntups = 0;
        for (int i = 0; i < numBuckets; ++i) {
            histogram[i] = 0;
        }
    }

    private int getIndex(int v) {
        int index = (v - minValue) / width;
        if (index >= numBuckets) {
            index = numBuckets - 1;
        }
        return index;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = getIndex(v);
        ++histogram[index];
        ++ntups;
    }

    private double eqSelec(int v) {
        if (v < minValue || v > maxValue) {
            return 0.0;
        }
        return histogram[getIndex(v)] / (double)width / (double)ntups;
    }

    private double gtSelec(int v) {
        if (v < minValue) {
            return 1.0;
        } else if (v > maxValue) {
            return 0.0;
        }
        int index = getIndex(v);
        int sum = 0;
        for (int i = index + 1; i < numBuckets; ++i) {
            sum += histogram[i];
        }
        int rightBoundary = minValue + index * width;
        double bucketFrequency = histogram[index] / (double) ntups;
        return (rightBoundary - v) / (double) width * bucketFrequency + sum / (double)ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        switch (op) {
            case EQUALS:
                return eqSelec(v);
            case NOT_EQUALS:
                return 1 - eqSelec(v);
            case GREATER_THAN:
                return gtSelec(v);
            case LESS_THAN:
                return 1 - eqSelec(v) - gtSelec(v);
            case LESS_THAN_OR_EQ:
                return 1 - gtSelec(v);
            case GREATER_THAN_OR_EQ:
                return eqSelec(v) + gtSelec(v);
                default:
                    return -1.0;
        }
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}

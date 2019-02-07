package simpledb;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    private ColumnStats[] stats;
    private TupleDesc td;
    private int ntups;
    private int ioCostPerPage;
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here
        this.ioCostPerPage = ioCostPerPage;
        ntups = 0;
        td = Database.getCatalog().getTupleDesc(tableid);
        initColumnStats();
        try {
            calculateMinMaxStats(tableid);
            calculateHistogram(tableid);
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    private void initColumnStats() {
        stats = new ColumnStats[td.numFields()];
        for (int i = 0; i < td.numFields(); ++i) {
            if (td.getType(i) == Type.INT_TYPE) {
                stats[i] = new IntColumnStats(NUM_HIST_BINS);
            } else if (td.getType(i) == Type.STRING_TYPE) {
                stats[i] = new StringColumnStats(NUM_HIST_BINS);
            } else {
                // TODO: error handling
            }
        }
    }

    private void calculateMinMaxStats(int tableid) throws DbException, TransactionAbortedException {
        SeqScan sc = new SeqScan(new TransactionId(), tableid, "");
        sc.open();
        while (sc.hasNext()) {
            Tuple tuple = sc.next();
            for (int i = 0; i < td.numFields(); ++i) {
                if (td.getType(i) == Type.INT_TYPE) {
                    IntField field = (IntField)tuple.getField(i);
                    IntColumnStats colStats = (IntColumnStats)stats[i];
                    if (field.getValue() < colStats.min) {
                        colStats.min = field.getValue();
                    }
                    if (field.getValue() > colStats.max) {
                        colStats.max = field.getValue();
                    }
                }
            }
            ++ntups;
        }
        sc.close();
    }

    private void calculateHistogram(int tableid) throws DbException, TransactionAbortedException {
        SeqScan sc = new SeqScan(new TransactionId(), tableid, "");
        sc.open();
        while (sc.hasNext()) {
            Tuple tuple = sc.next();
            for (int i = 0; i < td.numFields(); ++i) {
                if (td.getType(i) == Type.INT_TYPE) {
                    IntField field = (IntField)tuple.getField(i);
                    IntColumnStats colStats = (IntColumnStats)stats[i];
                    colStats.addValueToHistogram(field.getValue());
                } else if (td.getType(i) == Type.STRING_TYPE) {
                    StringField field = (StringField)tuple.getField(i);
                    StringColumnStats colStats = (StringColumnStats)stats[i];
                    colStats.addValueToHistogram(field.getValue());
                }
            }
        }
        sc.close();
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	// some code goes here
        return ntups * td.getSize()/ (double)BufferPool.PAGE_SIZE * ioCostPerPage;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)Math.floor(ntups * selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if (constant.getType() == Type.INT_TYPE) {
    	    IntField constantField = (IntField)constant;
    	    IntColumnStats columnStats = (IntColumnStats)stats[field];
    	    return columnStats.estimateSelectivity(op, constantField.getValue());
        } else if (constant.getType() == Type.STRING_TYPE) {
    	    StringField constantField = (StringField)constant;
    	    StringColumnStats columnStats = (StringColumnStats)stats[field];
    	    return columnStats.estimateSelectivity(op, constantField.getValue());
        }
        return 1.0;
    }

}

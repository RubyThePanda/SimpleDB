package simpledb;

/** Predicate compares tuples to a specified Field value.
 */
public class Predicate {

    /** Constants used for return codes in Field.compare */
    public enum Op {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public static String toString(Op op) {
            switch (op) {
                case EQUALS:
                    return "=";
                case NOT_EQUALS:
                    return "!=";
                case GREATER_THAN:
                    return ">";
                case GREATER_THAN_OR_EQ:
                    return ">=";
                case LESS_THAN:
                    return "<";
                case LESS_THAN_OR_EQ:
                    return "<=";
                case LIKE:
                    return "LIKE";
                default:
                        return "";
            }
        }
    }

    private int fieldId;
    private Op op;
    private Field operand;
    /**
     * Constructor.
     *
     * @param field field number of passed in tuples to compare against.
     * @param op operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // field number --> field index?
        this.fieldId = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific
     * in the constructor.  The comparison can be made through Field's
     * compare method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        Field field = t.getField(fieldId);
        return field.compare(op, operand);
    }

    /**
     * Returns something useful, like
     * "f = field_id op = op_string operand = operand_string
     */
    public String toString() {
        return "field[" + String.valueOf(fieldId) + "] " + Op.toString(op) + " " + operand.toString();
    }
}

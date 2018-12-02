package simpledb;
import java.io.*;

// 1) run dist:
// ant
// 2) convert a txt file into a binary file:
// java -jar dist/simpledb.jar convert some_data_file.txt 3
// 3) run SimpleQuery:
// java -classpath dist/simpledb.jar simpledb.SimpleQuery

public class SimpleQuery{

    static void testLab1() {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId(), "test");

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }

    static void testTableGrants() {
        // grants (id int, title string, amount int, org int, pi int, manager int, started string, ended string)
        Type types[] = new Type[]{
                Type.INT_TYPE, Type.STRING_TYPE,
                Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE,
                Type.STRING_TYPE, Type.STRING_TYPE};
        String names[] = new String[]{
                "id", "title",
                "amount", "org", "pi", "manager",
                "started", "ended"
        };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("grants.dat"), descriptor);
        Database.getCatalog().addTable(table1, "grants");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId(), "grants");

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }

    public static void main(String[] argv) {
//        SimpleQuery.testLab1();
        SimpleQuery.testTableGrants();
    }


}
package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;

public class StarField implements Field {
    private static Field singleton = new StarField();

    private StarField() {

    }
    public static Field getInstance() {
        return singleton;
    }
    @Override
    public void serialize(DataOutputStream dos) throws IOException {

    }

    @Override
    public boolean compare(Predicate.Op op, Field value) {
        return false;
    }

    @Override
    public Type getType() {
        return null;
    }
}

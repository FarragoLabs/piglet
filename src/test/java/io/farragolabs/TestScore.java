package io.farragolabs;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class TestScore extends EvalFunc<String> {
    @Override
    public String exec(Tuple tuple) throws IOException {
        return tuple.get(0).toString();
    }
}

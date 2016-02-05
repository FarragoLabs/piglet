package io.farragolabs;

import io.farragolabs.pig.storage.File;
import io.farragolabs.pig.storage.HBase;
import org.junit.Test;
import io.farragolabs.pig.*;

import java.util.HashMap;
import java.util.Map;

public class PigTest {

    @Test
    public void pigLoadFileStatement()
    {
        Pig pig = Pig.instance();

        pig.load("test")
                .dump();
    }

    @Test
    public void groupByShouldWork()
    {
        Pig pig = Pig.instance();
        pig.load("test")
                .groupBy("$0")
                .dump();
    }

    @Test
    public void orderbyShouldWork()
    {
        Pig pig = Pig.instance();
        pig.load("test")
                .groupBy("$0")
                .orderBy("$0")
                .dump();
    }

    @Test
    public void fileStorageShouldWork()
    {
        Pig pig = Pig.instance();

        pig.load(new File("test_2","first,second",","))
                .foreachGenerate("first")
                .groupBy("first")
                .orderBy(PigConstants.GROUP)
                .dump();
    }

    @Test
    public void hbaseStorageShouldWork()
    {
        Pig pig = Pig.instance();

        Map<String, String> schema = new HashMap<String, String>();

        pig.load(new HBase("test_2",schema))
                .foreachGenerate("first")
                .groupBy("first")
                .orderBy(PigConstants.GROUP)
                .dump();
    }


}

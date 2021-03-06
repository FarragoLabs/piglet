package io.farragolabs;

import io.farragolabs.pig.filter.Filter;
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
        Pig pig = Pig.instance("test");
        pig.load("test")
                .dump();
    }

    @Test
    public void groupByShouldWork()
    {
        Pig pig = Pig.instance("test");
        pig.load("test")
                .groupBy("$0")
                .dump();
    }

    @Test
    public void orderbyShouldWork()
    {
        Pig pig = Pig.instance("test");
        pig.load("test")
                .groupBy("$0")
                .orderBy("$0")
                .dump();
    }

    @Test
    public void fileStorageWithFiltersShouldWork()
    {
        Pig pig = Pig.instance("test");

        pig.load(new File("test_2","first,second",","))
                .foreachGenerate("first")
                .filter(
                        new Filter("first", "==", "'asd'")
                                .and("first", "==", "'asd'")
                                .or("first", ">=", "'asd'")
                )
                .groupBy("first")
                .orderBy(PigConstants.GROUP)
                .dump();
    }

    @Test
    public void hbaseStorageShouldWork()
    {
        Pig pig = Pig.instance("test");

        Map<String, String> schema = new HashMap<String, String>();

        pig.load(new HBase("test_2", schema))
                .foreachGenerate("first")
                .groupBy("first")
                .orderBy(PigConstants.GROUP)
                .dump();
    }

    @Test
    public void udfs()
    {
        Pig pig = Pig.instance("test")
                .register()
                .define("TEST_SCORE",TestScore.class);

        pig.load(new File("test_2","first,second",","))
                .foreachGenerate("TEST_SCORE(first)")
                .groupBy("first")
                .orderBy(PigConstants.GROUP)
                .dump();
    }


    @Test
    public void join()
    {
        Pig pig = Pig.instance("test")
                .register()
                .define("TEST_SCORE", TestScore.class);

        Pig otherDataSet = Pig.instance("OtherDataSet");

        otherDataSet.load("test").orderBy("$0");

        pig.load(new File("test_2", "first,second", ","))
                .foreachGenerate("TEST_SCORE(first)")
                .groupBy("first")
                .join("first",new Joinable(otherDataSet,"$0"))
                .orderBy(PigConstants.GROUP)
                .dump();
    }
}

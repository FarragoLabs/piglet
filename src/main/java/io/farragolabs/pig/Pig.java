package io.farragolabs.pig;

import io.farragolabs.pig.filter.IFilter;
import io.farragolabs.pig.storage.Storage;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

public class Pig {

    private List<String> statements;
    private AutoAlphabet autoAlphabet;
    private boolean mapreduce;
    private static String lastVariable;

    private Pig(ArrayList<String> statements, AutoAlphabet autoAlphabet) {

        this.statements = statements;
        this.autoAlphabet = autoAlphabet;
    }

    public static Pig instance() {
        return new Pig(new ArrayList<String>(), new AutoAlphabet());
    }

    public Pig load(Storage storage) {
        lastVariable = autoAlphabet.next();
        statements.add(String.format("%s = %s", lastVariable, storage.statement()));
        return this;
    }

    public Pig load(String fileLocation) {
        lastVariable = autoAlphabet.next();
        statements.add(String.format("%s = LOAD '%s';", lastVariable, fileLocation));
        return this;
    }

    public void dump() {
        statements.add(String.format("DUMP %s", lastVariable));

        String join = StringUtils.join(statements, "\n");

        System.out.println(join);

        // OSCall.pig(join);
    }

    public Pig groupBy(String... variables) {
        String currentVariable = autoAlphabet.next();
        String format = String.format("%s = GROUP %s by %s;", currentVariable, lastVariable, "(" + StringUtils.join(variables, ",") + ")");
        lastVariable = currentVariable;
        statements.add(format);
        return this;
    }

    public Pig filter(IFilter filter) {
        String currentVariable = autoAlphabet.next();
        String format = String.format("%s = FILTER %s by %s;", currentVariable, lastVariable, filter.statement());
        lastVariable = currentVariable;
        statements.add(format);
        return this;
    }

    public Pig orderBy(String... variables) {
        String currentVariable = autoAlphabet.next();
        String format = String.format("%s = ORDER %s by %s;", currentVariable, lastVariable, "(" + StringUtils.join(variables, ",") + ")");
        statements.add(format);
        lastVariable = currentVariable;
        return this;
    }

    public Pig foreachGenerate(String... variables) {

        String currentVariable = autoAlphabet.next();
        String format = String.format("%s = FOREACH %s GENERATE %s;", currentVariable, lastVariable, StringUtils.join(variables, ","));
        statements.add(format);
        lastVariable = currentVariable;

        return this;
    }

    public <T> Pig define(String test_score, Class<T> udfClass) {
        statements.add(String.format("DEFINE %s %s;", test_score, udfClass.getName()));
        return this;
    }

    public boolean validate() {
        String join = StringUtils.join(statements, "\n");

        // OSCall.shell(String.format("pig -e '%s'",join));

        return true;

    }

    public Pig register() {
        try {
            String path = new File(this.getClass().getProtectionDomain()
                    .getCodeSource()
                    .getLocation().toURI()).getPath();
            String decodedPath = URLDecoder.decode(path, "UTF-8");

            statements.add("REGISTER " + path);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return this;
    }

    public Pig register(String path) {
        statements.add("REGISTER " + path);
        return this;
    }
}

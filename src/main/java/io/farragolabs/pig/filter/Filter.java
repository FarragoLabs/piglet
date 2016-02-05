package io.farragolabs.pig.filter;

public class Filter implements IFilter {

    private String statement = "";

    public Filter(String variable, String operator, String value) {

        this.statement = "( "+variable+" "+operator+" "+value+" )";
    }

    public Filter(String variable, String operator, String value, boolean not) {

        if(not)
        {
            this.statement = "NOT ( "+variable+" "+operator+" "+value+" )";
        }
        else
        {
            this.statement = "( "+variable+" "+operator+" "+value+" )";
        }
    }

    public String statement() {
        return statement;
    }

    public Filter and(String variable, String operator, String value)
    {
        this.statement = statement + String.format(" AND ( %s %s %s ) ",variable,operator,value);
        return this;
    }

    public Filter or(String variable, String operator, String value)
    {
        this.statement = statement + String.format(" OR ( %s %s %s ) ",variable,operator,value);
        return this;
    }
}

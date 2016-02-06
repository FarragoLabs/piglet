package io.farragolabs.pig;

public class Joinable {
    private Pig pig;
    private final String column_list;

    public Joinable(Pig pig, String column_list) {

        this.pig = pig;
        this.column_list = column_list;
    }

    public Pig getPig() {
        return pig;
    }

    public String getColumn_list() {
        return column_list;
    }
}

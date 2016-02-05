package io.farragolabs.pig.storage;

public class File implements Storage {
    private final String fileName;
    private final String schema;
    private String delimiter;

    public File(String fileName, String schema) {
        this.fileName = fileName;
        this.schema = schema;
        this.delimiter = "/t";
    }

    public File(String fileName, String schema, String delimiter) {
        this.fileName = fileName;
        this.schema = schema;
        this.delimiter = delimiter;
    }

    public String statement() {
        return String.format("LOAD '%s' using PigStorage('%s') as (%s);",fileName,delimiter,schema);
    }
}

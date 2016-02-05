package io.farragolabs.pig.storage;

import java.util.Map;

public class HBase implements Storage {
    private final String tableName;
    private final Map<String, String> schema;
    private final boolean loadKey;

    public HBase(String tableName, Map<String, String> schema) {

        this.tableName = tableName;
        this.schema = schema;
        this.loadKey = false;
    }

    public HBase(String tableName, Map<String, String> schema, boolean loadKey) {

        this.tableName = tableName;
        this.schema = schema;
        this.loadKey = loadKey;
    }

    public String statement() {
        return null;
    }
}

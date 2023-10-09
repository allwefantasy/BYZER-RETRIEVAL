package tech.mlsql.retrieval.records;

import java.io.Serializable;
import java.util.Optional;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record TableSettings(String database,
//                            Optional<String> table,
//                            String schema,
//                            Optional<String> location,
//                            int num_shards)  {
//}

public class TableSettings implements Serializable {
    private String database;
    private String table;
    private String schema;
    private String location;
    private int num_shards;

    public TableSettings(String database, String table, String schema, String location, int num_shards) {
        this.database = database;
        this.table = table;
        this.schema = schema;
        this.location = location;
        this.num_shards = num_shards;
    }

    public TableSettings() {
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getNum_shards() {
        return num_shards;
    }

    public void setNum_shards(int num_shards) {
        this.num_shards = num_shards;
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
    }

    public String schema() {
        return schema;
    }

    public String location() {
        return location;
    }

    public int num_shards() {
        return num_shards;
    }
}

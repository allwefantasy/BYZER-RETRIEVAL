package tech.mlsql.retrieval.records;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record SearchQuery(
//                          Optional<String> keyword,
//                          List<String> fields, float[] vector,
//                          Optional<String> vectorField,
//                          int limit) {
//}

public class SearchQuery implements Serializable {
    private String database;
    private String table;
    private Optional<String> keyword;
    private List<String> fields;
    private float[] vector;
    private Optional<String> vectorField;
    private int limit;

    public SearchQuery() {
    }

    public Optional<String> getKeyword() {
        return keyword;
    }

    public void setKeyword(Optional<String> keyword) {
        this.keyword = keyword;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public float[] getVector() {
        return vector;
    }

    public void setVector(float[] vector) {
        this.vector = vector;
    }

    public Optional<String> getVectorField() {
        return vectorField;
    }

    public void setVectorField(Optional<String> vectorField) {
        this.vectorField = vectorField;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public SearchQuery(String database, String table,Optional<String> keyword, List<String> fields, float[] vector, Optional<String> vectorField, int limit) {
        this.database = database;
        this.table = table;
        this.keyword = keyword;
        this.fields = fields;
        this.vector = vector;
        this.vectorField = vectorField;
        this.limit = limit;
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

    public Optional<String> keyword() {
        return keyword;
    }

    public List<String> fields() {
        return fields;
    }

    public float[] vector() {
        return vector;
    }

    public Optional<String> vectorField() {
        return vectorField;
    }

    public int limit() {
        return limit;
    }
}

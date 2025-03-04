package tech.mlsql.retrieval.records;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Represents the result of a search query
 */
public class SearchQueryResult implements Serializable {
    private String database;
    private String table;
    private List<Map<String, Object>> results;
    private long totalHits;
    private float maxScore;

    public SearchQueryResult() {
    }

    public SearchQueryResult(String database, String table, 
                           List<Map<String, Object>> results, 
                           long totalHits, float maxScore) {
        this.database = database;
        this.table = table;
        this.results = results;
        this.totalHits = totalHits;
        this.maxScore = maxScore;
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

    public List<Map<String, Object>> getResults() {
        return results;
    }

    public void setResults(List<Map<String, Object>> results) {
        this.results = results;
    }

    public long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(long totalHits) {
        this.totalHits = totalHits;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = maxScore;
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
    }

    public List<Map<String, Object>> results() {
        return results;
    }

    public long totalHits() {
        return totalHits;
    }

    public float maxScore() {
        return maxScore;
    }
}
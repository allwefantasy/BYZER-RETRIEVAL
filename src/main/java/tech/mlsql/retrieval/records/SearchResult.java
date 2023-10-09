package tech.mlsql.retrieval.records;

import org.apache.lucene.document.Document;

import java.io.Serializable;
import java.util.Map;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record SearchResult(float score, Document doc) {
//}

public class SearchResult implements Serializable {
    private float score;
    private Map<String,Object> doc;

    public SearchResult(float score, Map<String,Object> doc) {
        this.score = score;
        this.doc = doc;
    }

    public SearchResult() {
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public Map<String,Object> getDoc() {
        return doc;
    }

    public void setDoc(Map<String,Object> doc) {
        this.doc = doc;
    }

    public float score() {
        return score;
    }

    public Map<String,Object> doc() {
        return doc;
    }
}

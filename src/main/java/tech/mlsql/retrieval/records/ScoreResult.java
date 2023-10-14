package tech.mlsql.retrieval.records;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 10/14/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class ScoreResult implements Serializable {
    public ScoreResult() {
    }

    private Map<Object,Float> newScores;
    private Map<Object, Map<String,Object>> idToDocs;

    public ScoreResult(Map<Object, Float> newScores, Map<Object, Map<String, Object>> idToDocs) {
        this.newScores = newScores;
        this.idToDocs = idToDocs;
    }

    public Map<Object, Float> getNewScores() {
        return newScores;
    }

    public void setNewScores(Map<Object, Float> newScores) {
        this.newScores = newScores;
    }

    public Map<Object, Map<String, Object>> getIdToDocs() {
        return idToDocs;
    }

    public void setIdToDocs(Map<Object, Map<String, Object>> idToDocs) {
        this.idToDocs = idToDocs;
    }
}

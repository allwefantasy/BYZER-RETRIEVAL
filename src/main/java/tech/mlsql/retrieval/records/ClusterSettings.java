package tech.mlsql.retrieval.records;

import java.io.Serializable;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record ClusterSettings(String name,String location) {
//}

public class ClusterSettings implements Serializable {
    private  String name;
    private  String location;

    private int numNodes = 1;

    public ClusterSettings(String name, String location, int numNodes) {
        this.name = name;
        this.location = location;
        this.numNodes = numNodes;
    }

    public ClusterSettings() {

    }

    public int getNumNodes() {
        return numNodes;
    }

    public void setNumNodes(int numNodes) {
        this.numNodes = numNodes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String name() {
        return name;
    }

    public String location() {
        return location;
    }
}

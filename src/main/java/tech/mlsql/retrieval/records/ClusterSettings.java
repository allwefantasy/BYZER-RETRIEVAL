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

    public ClusterSettings(String name, String location) {
        this.name = name;
        this.location = location;
    }

    public ClusterSettings() {

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

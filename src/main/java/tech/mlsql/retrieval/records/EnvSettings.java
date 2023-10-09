package tech.mlsql.retrieval.records;

import java.io.Serializable;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record EnvSettings(String javaHome, String path) {
//}

public class EnvSettings implements Serializable {
    private  String javaHome;
    private  String path;

    public EnvSettings(String javaHome, String path) {
        this.javaHome = javaHome;
        this.path = path;
    }

    public EnvSettings() {
    }

    public String getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(String javaHome) {
        this.javaHome = javaHome;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String javaHome() {
        return javaHome;
    }

    public String path() {
        return path;
    }
}

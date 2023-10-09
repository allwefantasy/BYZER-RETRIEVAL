package tech.mlsql.retrieval.records;

import java.io.Serializable;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record ClusterInfo(ClusterSettings clusterSettings,TableSettings tableSettings,JVMSettings jvmSettings,EnvSettings envSettings) {
//}

public class ClusterInfo implements Serializable {
    private ClusterSettings clusterSettings;
    private TableSettings tableSettings;
    private JVMSettings jvmSettings;
    private EnvSettings envSettings;

    public ClusterInfo(ClusterSettings clusterSettings, TableSettings tableSettings, JVMSettings jvmSettings, EnvSettings envSettings) {
        this.clusterSettings = clusterSettings;
        this.tableSettings = tableSettings;
        this.jvmSettings = jvmSettings;
        this.envSettings = envSettings;
    }

    public ClusterInfo() {
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public void setClusterSettings(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    public TableSettings getTableSettings() {
        return tableSettings;
    }

    public void setTableSettings(TableSettings tableSettings) {
        this.tableSettings = tableSettings;
    }

    public JVMSettings getJvmSettings() {
        return jvmSettings;
    }

    public void setJvmSettings(JVMSettings jvmSettings) {
        this.jvmSettings = jvmSettings;
    }

    public EnvSettings getEnvSettings() {
        return envSettings;
    }

    public void setEnvSettings(EnvSettings envSettings) {
        this.envSettings = envSettings;
    }

    public ClusterSettings clusterSettings() {
        return clusterSettings;
    }

    public TableSettings tableSettings() {
        return tableSettings;
    }

    public JVMSettings jvmSettings() {
        return jvmSettings;
    }

    public EnvSettings envSettings() {
        return envSettings;
    }
}

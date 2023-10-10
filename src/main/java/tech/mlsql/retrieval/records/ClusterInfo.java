package tech.mlsql.retrieval.records;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record ClusterInfo(ClusterSettings clusterSettings,TableSettings tableSettings,JVMSettings jvmSettings,EnvSettings envSettings) {
//}

public class ClusterInfo implements Serializable {
    private ClusterSettings clusterSettings;
    private JVMSettings jvmSettings;
    private EnvSettings envSettings;

    private List<TableSettings> tableSettingsList = new ArrayList<>();

    public ClusterInfo(ClusterSettings clusterSettings, JVMSettings jvmSettings, EnvSettings envSettings) {
        this.clusterSettings = clusterSettings;
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

    public List<TableSettings> tableSettingsList() {
        return tableSettingsList;
    }

    public void addTableSettings(TableSettings tableSettings) {
        this.tableSettingsList.add(tableSettings);
    }

    public JVMSettings jvmSettings() {
        return jvmSettings;
    }

    public EnvSettings envSettings() {
        return envSettings;
    }
}

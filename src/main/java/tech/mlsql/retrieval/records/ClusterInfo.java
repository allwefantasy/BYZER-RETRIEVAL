package tech.mlsql.retrieval.records;

import java.io.Serializable;
import java.util.*;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record ClusterInfo(ClusterSettings clusterSettings,TableSettings tableSettings,JVMSettings jvmSettings,EnvSettings envSettings) {
//}

public class ClusterInfo implements Serializable {
    private ClusterSettings clusterSettings;
    private JVMSettings jvmSettings;
    private EnvSettings envSettings;

    private ResourceRequirementSettings resourceRequirementSettings;

    private List<TableSettings> tableSettingsList = new ArrayList<>();


    // this is a map from worker index to worker location(e.g. ip).
    // it's used to restore worker when the cluster is restarted.
    // so there is no need to set it in constructor.
    private Map<Integer,String> workerLocations = new HashMap<>();



    public ClusterInfo(ClusterSettings clusterSettings,
                       JVMSettings jvmSettings,
                       EnvSettings envSettings,
                       ResourceRequirementSettings resourceRequirementSettings ) {
        this.clusterSettings = clusterSettings;
        this.jvmSettings = jvmSettings;
        this.envSettings = envSettings;
        this.resourceRequirementSettings = resourceRequirementSettings;
    }

    public Map<Integer,String> getWorkerLocations() {
        return workerLocations;
    }

    public void setWorkerLocations(Map<Integer,String> workerLocations) {
        this.workerLocations = workerLocations;
    }

    public void addWorkerLocation(int workerIndex, String workerLocation) {
        this.workerLocations.put(workerIndex, workerLocation);
    }

    public ResourceRequirementSettings getResourceRequirementSettings() {
        return resourceRequirementSettings;
    }

    public void setResourceRequirementSettings(ResourceRequirementSettings resourceRequirementSettings) {
        this.resourceRequirementSettings = resourceRequirementSettings;
    }

    public List<TableSettings> getTableSettingsList() {
        return tableSettingsList;
    }

    public void setTableSettingsList(List<TableSettings> tableSettingsList) {
        this.tableSettingsList = tableSettingsList;
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
    public void removeTableSettings(TableSettings tableSettings) {
        var targetTableSettings = this.tableSettingsList.stream().filter(item -> item.database().equals(tableSettings.database()) && item.table().equals(tableSettings.table())).findFirst();
        if (targetTableSettings.isPresent()) {
            this.tableSettingsList.remove(targetTableSettings.get());
        }
    }

    public void removeTableSettings(String database, String table) {
        var targetTableSettings = this.tableSettingsList.stream().filter(item -> item.database().equals(database) &&
                item.table().equals(table)).findFirst();
        if (targetTableSettings.isPresent()) {
            this.tableSettingsList.remove(targetTableSettings.get());
        }
    }

    public Optional<TableSettings> findTableSettings(String database, String table) {
        var targetTableSettings = this.tableSettingsList.stream().filter(item -> item.database().equals(database) &&
                item.table().equals(table)).findFirst();
        return targetTableSettings;
    }

    public JVMSettings jvmSettings() {
        return jvmSettings;
    }

    public EnvSettings envSettings() {
        return envSettings;
    }
}

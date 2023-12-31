package tech.mlsql.retrieval;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.options.ActorLifetime;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.api.runtimeenv.RuntimeEnvConfig;
import io.ray.api.runtimeenv.types.RuntimeEnvName;
import tech.mlsql.retrieval.records.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tech.mlsql.retrieval.Utils.toRecord;

/**
 * 10/6/23 WilliamZhu(allwefantasy@gmail.com)
 * This is a gateway which will called by python code.
 * Notice that:
 * 1. The method in this class should have a return value. Otherwise, the python code call it will crash this actor.
 */
public class RetrievalGateway {

    private List<ClusterInfo> clusterInfos;

    public RetrievalGateway() {
        this.clusterInfos = new ArrayList<>();
    }

    public boolean buildCluster(String clusterSettingsStr,
                                String envSettingsStr,
                                String JVMSettingsStr,
                                String resourceRequirementSettingsStr) throws Exception {

        var clusterSettings = toRecord(clusterSettingsStr, ClusterSettings.class);

        var envSettings = toRecord(envSettingsStr, EnvSettings.class);
        // --enable-preview --add-modules jdk.incubator.vector
        var jvmSettings = toRecord(JVMSettingsStr, JVMSettings.class);

        jvmSettings.options().addAll(Utils.defaultJvmOptions());

        if (!jvmSettings.options().contains("--enable-preview")) {
            jvmSettings.options().add("--enable-preview");
        }

        if (!jvmSettings.options().contains("jdk.incubator.vector")) {
            jvmSettings.options().add("--add-modules");
            jvmSettings.options().add("jdk.incubator.vector");
        }

//        //--add-modules jdk.incubator.foreign
//        if (!jvmSettings.options().contains("jdk.incubator.foreign")) {
//            jvmSettings.options().add("--add-modules");
//            jvmSettings.options().add("jdk.incubator.foreign");
//        }

        var runtimeEnv = new RuntimeEnv.Builder().build();
        Map<String, String> envMap = new HashMap<>();
        envMap.put("JAVA_HOME", envSettings.javaHome());
        envMap.put("PATH", envSettings.path());
        runtimeEnv.set(RuntimeEnvName.ENV_VARS, envMap);

        var resourceRequirementSettings = Utils.toRecord(resourceRequirementSettingsStr, ResourceRequirementSettings.class);
        var clusterInfo = new ClusterInfo(clusterSettings, jvmSettings, envSettings, resourceRequirementSettings);

        var actor = Ray.actor(RetrievalMaster::new, clusterInfo).
                setName(clusterSettings.name()).
                setLifetime(ActorLifetime.DETACHED).
                setRuntimeEnv(runtimeEnv).
                setJvmOptions(jvmSettings.options());

        for (var item : clusterInfo.getResourceRequirementSettings().getResourceRequirements()) {
            actor.setResource(item.getName(), item.getResourceQuantity());
        }

        actor.remote();

        this.clusterInfos.add(clusterInfo);

        return true;
    }

    // Notice that when restore the cluster, we need the worker location(which ip the worker is running on).
    // if the cluster use local disk instead of shared file system, then the worker location is not needed.
    public boolean restoreFromClusterInfo(String clusterInfoStr) throws Exception {
        var clusterInfo = toRecord(clusterInfoStr, ClusterInfo.class);
        var clusterSettings = clusterInfo.clusterSettings();

        var envSettings = clusterInfo.envSettings();
        // --enable-preview --add-modules jdk.incubator.vector
        var jvmSettings = clusterInfo.getJvmSettings();


        if (jvmSettings.options().isEmpty()) {
            jvmSettings.options().addAll(Utils.defaultJvmOptions());
        }

        if (!jvmSettings.options().contains("--enable-preview")) {
            jvmSettings.options().add("--enable-preview");
        }

        if (!jvmSettings.options().contains("jdk.incubator.vector")) {
            jvmSettings.options().add("--add-modules");
            jvmSettings.options().add("jdk.incubator.vector");
        }

//        //--add-modules jdk.incubator.foreign
//        if (!jvmSettings.options().contains("jdk.incubator.foreign")) {
//            jvmSettings.options().add("--add-modules");
//            jvmSettings.options().add("jdk.incubator.foreign");
//        }

        var runtimeEnv = new RuntimeEnv.Builder().build();
        Map<String, String> envMap = new HashMap<>();
        envMap.put("JAVA_HOME", envSettings.javaHome());
        envMap.put("PATH", envSettings.path());
        runtimeEnv.set(RuntimeEnvName.ENV_VARS, envMap);

        var actor = Ray.actor(RetrievalMaster::new, clusterInfo).
                setName(clusterSettings.name()).
                setLifetime(ActorLifetime.DETACHED).
                setRuntimeEnv(runtimeEnv).
                setJvmOptions(jvmSettings.options());

        for (var item : clusterInfo.getResourceRequirementSettings().getResourceRequirements()) {
            actor.setResource(item.getName(), item.getResourceQuantity());
        }

        actor.remote();

        this.clusterInfos.add(clusterInfo);
        return true;
    }

    public ActorHandle<RetrievalMaster> getCluster(String clusterName) {
        return (ActorHandle<RetrievalMaster>) Ray.getActor(clusterName).get();
    }

    // for now we will just shutdown the cluster and
    // remove the cluster info from clusterInfos.
    // We will not close all tables belong to the cluster and clean the index files
    public boolean shutdownCluster(String clusterName) {
        var cluster = getCluster(clusterName);
        try {
            cluster.task(RetrievalMaster::shutdown).remote();
        } catch (Exception e) {
            // ignore
        }

        this.clusterInfos.removeIf(item -> item.clusterSettings().name().equals(clusterName));
        return true;
    }
}

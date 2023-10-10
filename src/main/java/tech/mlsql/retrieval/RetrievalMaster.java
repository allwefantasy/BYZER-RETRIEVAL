package tech.mlsql.retrieval;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.api.runtimeenv.types.RuntimeEnvName;
import org.apache.lucene.document.Document;
import tech.mlsql.retrieval.records.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 10/6/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class RetrievalMaster {
    private List<ActorHandle<RetrievalWorker>> workers = new ArrayList<>();

    public RetrievalMaster(ClusterInfo clusterInfo) {
        var clusterSettings = clusterInfo.clusterSettings();
        var envSettings = clusterInfo.envSettings();


        var runtimeEnv = new RuntimeEnv.Builder().build();
        Map<String, String> envMap = new HashMap<>();
        envMap.put("JAVA_HOME", envSettings.javaHome());
        envMap.put("PATH", envSettings.path());
        runtimeEnv.set(RuntimeEnvName.ENV_VARS, envMap);

        for (int i = 0; i < clusterSettings.getNumNodes(); i++) {
            workers.add(
                    Ray.actor(RetrievalWorker::new, clusterSettings).
                            setName(clusterSettings.name() + "-worker").
                            setRuntimeEnv(runtimeEnv).
                            setJvmOptions(clusterInfo.jvmSettings().options())
                            .remote());
        }
    }

    public boolean createTable(String tableSettingStr) throws Exception {
        var tableSettings = Utils.toRecord(tableSettingStr, TableSettings.class);
        var tasks = new ArrayList<ObjectRef<Boolean>>();
        for (var worker : workers) {
            var ref = worker.task(RetrievalWorker::createTable, tableSettings).remote();
            tasks.add(ref);
        }
        Ray.get(tasks);
        return true;
    }

    // just for test
    public boolean build(String database, String table, String dataLocationStr) throws Exception {
        var tasks = new ArrayList<ObjectRef<Boolean>>();
        for (var worker : workers) {
            var ref = worker.task(RetrievalWorker::build, database, table, dataLocationStr).remote();
            tasks.add(ref);
        }
        Ray.get(tasks);
        return true;
    }

    public boolean buildFromRayObjectStore(String database, String table, byte[][] batchData,byte[][] locations) throws Exception {
        var tasks = new ArrayList<ObjectRef<Boolean>>();
        // split batchData to shards, which the number of shards is equal to the number of workers
        var batchDataShards = new ArrayList<List<ObjectRef<String>>>();
        for (int i = 0; i < workers.size(); i++) {
            batchDataShards.add(new ArrayList<>());
        }
        for (int i = 0; i < batchData.length; i++) {

            // get record from ray object store
            var objRefId = batchData[i];
            var location = locations[i];
            var objRef = Utils.readBinaryAsObjectRef(objRefId,String.class,location);
            var row = Ray.get(objRef);

            // deserialize record json string to map
            var data = Utils.toRecord(row, Map.class);
            if (data.containsKey("_id")) {
                var id = data.get("_id");
                if (id instanceof Long) {
                    Long shardId = (Long) id % workers.size();
                    batchDataShards.get(shardId.intValue()).add(objRef);
                } else {
                    var shardId = id.toString().hashCode() % workers.size();
                    batchDataShards.get(shardId).add(objRef);
                }
            } else {
                throw new Exception("The data does not contain _id field");
            }
        }
        for (int i = 0; i < workers.size(); i++) {
            var worker = workers.get(i);
            var ref = worker.task(RetrievalWorker::buildFromRayObjectStore, database, table, batchDataShards.get(i)).remote();
            tasks.add(ref);
        }

        Ray.get(tasks);
        return true;
    }

    public String search(String database, String table, String queryStr) throws Exception {
        var tasks = new ArrayList<ObjectRef<List<SearchResult>>>();
        for (var worker : workers) {
            var ref = worker.task(RetrievalWorker::search, database, table, queryStr).remote();
            tasks.add(ref);
        }
        List<SearchResult> result = Ray.get(tasks).stream().flatMap(r -> r.stream()).collect(Collectors.toList());
        result.sort((o1, o2) -> {
            var score1 = o1.score();
            var score2 = o2.score();
            return Float.compare(score2, score1);
        });
        SearchQuery query = Utils.toRecord(queryStr, SearchQuery.class);

        var jsonResult = new ArrayList<Map<String, Object>>();
        for (var item : result.stream().limit(query.limit()).collect(Collectors.toList())) {
            var doc = item.doc();
            doc.put("_score", item.score());
            jsonResult.add(doc);
        }
        return Utils.toJson(jsonResult);
    }

    // commit the target table index
    public void commit(String database, String table) throws Exception {
        var tasks = new ArrayList<ObjectRef<Long>>();
        for (var worker : this.workers) {
            var ref = worker.task(RetrievalWorker::commit, database, table).remote();
            tasks.add(ref);
        }
        Ray.get(tasks);
    }
}

package tech.mlsql.retrieval;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 10/6/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class RetrievalMaster {
    private List<ActorHandle<RetrievalWorker>> workers = new ArrayList<>();
    private ClusterInfo clusterInfo;

    public RetrievalMaster(ClusterInfo clusterInfo) {
        var clusterSettings = clusterInfo.clusterSettings();
        var envSettings = clusterInfo.envSettings();


        var runtimeEnv = new RuntimeEnv.Builder().build();
        Map<String, String> envMap = new HashMap<>();
        envMap.put("JAVA_HOME", envSettings.javaHome());
        envMap.put("PATH", envSettings.path());
        runtimeEnv.set(RuntimeEnvName.ENV_VARS, envMap);

        for (int i = 0; i < clusterSettings.getNumNodes(); i++) {
            var actor = Ray.actor(RetrievalWorker::new, clusterInfo, i);
            actor.setName(clusterSettings.name() + "-worker-"+i).
                    setRuntimeEnv(runtimeEnv).
                    setJvmOptions(clusterInfo.jvmSettings().options());


            for (var entry : clusterInfo.getResourceRequirementSettings().getResourceRequirements()) {
                actor.setResource(entry.getName(), entry.getResourceQuantity());
            }

            // if the worker location is not null, then set the worker location
            // to force the worker to run on the specified node.
            if (clusterInfo.getWorkerLocations().containsKey(i)) {
                actor.setResource("node:" + clusterInfo.getWorkerLocations().get(i), 1.0);
            }

            workers.add(actor.remote());
        }

        // new cluster, not restore from checkpoint
        // try to get worker location from worker actor
        if (clusterInfo.getWorkerLocations().isEmpty()) {
            for (int i = 0; i < clusterSettings.getNumNodes(); i++) {
                var worker = workers.get(i);
                clusterInfo.addWorkerLocation(i, Ray.get(worker.task(RetrievalWorker::getNode).remote()));
            }
        }
        this.clusterInfo = clusterInfo;
    }

    public String clusterInfo() throws Exception {
        return Utils.toJson(clusterInfo);
    }

    public boolean createTable(String tableSettingStr) throws Exception {
        var tableSettings = Utils.toRecord(tableSettingStr, TableSettings.class);
        var tasks = new ArrayList<ObjectRef<Boolean>>();
        for (var worker : workers) {
            var ref = worker.task(RetrievalWorker::createTable, tableSettings).remote();
            tasks.add(ref);
        }
        Ray.get(tasks);
        this.clusterInfo.tableSettingsList().add(tableSettings);
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

    public boolean buildFromRayObjectStore(String database, String table, byte[][] batchData, byte[][] locations) throws Exception {
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
            var objRef = Utils.readBinaryAsObjectRef(objRefId, String.class, location);
            var row = Ray.get(objRef);

            // deserialize record json string to map
            var data = Utils.toRecord(row, Map.class);
            if (data.containsKey("_id")) {
                var id = data.get("_id");
                var shardId = Utils.route(id, workers.size());
                batchDataShards.get(shardId).add(objRef);
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

    /**
     *
     * @param database
     * @param table
     * @param searchQuery
     * @return
     * @throws JsonProcessingException
     *
     *
     * The caller should make sure the searchQuery contains full-text search or vector search and not be mixed.
     */
    private List<SearchResult> inner_search(String database, String table, SearchQuery searchQuery) throws JsonProcessingException {
        var tasks = new ArrayList<ObjectRef<List<SearchResult>>>();
        for (var worker : workers) {
            var ref = worker.task(RetrievalWorker::search, database, table, Utils.toJson(searchQuery)).remote();
            tasks.add(ref);
        }
        List<SearchResult> result = Ray.get(tasks).stream().flatMap(r -> r.stream()).collect(Collectors.toList());

        // The result from different workers , and the score is comparable (full-text search or vector search)
        // So we need to sort the result by score descent.
        result.sort((o1, o2) -> {
            var score1 = o1.score();
            var score2 = o2.score();
            return Float.compare(score2, score1);
        });

        return result;
    }

    // In the future, we should extract the isReciprocalRankFusion as an enum
    // since we may provide many algorithms for fusion.
    private ScoreResult singleRecall(String database, String table, SearchQuery tempQuery, boolean isReciprocalRankFusion) throws Exception {

        Map<Object, Float> newScores = new HashMap<>();
        Map<Object, Map<String, Object>> idToDocs = new HashMap<>();

        List<SearchResult> result = inner_search(database, table, tempQuery);
        if (isReciprocalRankFusion) {
            for (int i = 0; i < result.size(); i++) {
                // this algorithm is simple for now.
                // maybe later we can refer to this: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/MSR-TR-2010-82.pdf
                var item = result.get(i);
                var doc = item.doc();
                var id = doc.get("_id");
                if (!newScores.containsKey(doc)) {
                    newScores.put(id, 0.0f);
                }
                var previewScore = newScores.get(id);
                var updatedScore = previewScore + 1.0f / (i + 60.0f);
                newScores.put(id, updatedScore);
                idToDocs.put(id, doc);
            }
        } else {
            for (var item : result) {
                var doc = item.doc();
                var id = doc.get("_id");
                newScores.put(id, item.score());
                idToDocs.put(id, doc);
            }
        }
        return new ScoreResult(newScores, idToDocs);
    }

    public String search(String queryStr) throws Exception {

        List<SearchQuery> queries = Utils.toSearchQueryList(queryStr);
        List<ScoreResult> scoreResults = new ArrayList<>();

        var sampleQuery = queries.get(0);

        try (var executors = Executors.newVirtualThreadPerTaskExecutor()) {

            List<Future<ScoreResult>> responses = new ArrayList<>();
            for (var query : queries) {
                boolean isReciprocalRankFusion = query.keyword().isPresent() && query.vectorField().isPresent();

                if (query.keyword().isPresent()) {
                    var response = executors.submit(() -> {
                        var tempQuery = new SearchQuery(query.getDatabase(), query.getTable(), query.keyword(), query.fields(), query.vector(), Optional.empty(), query.limit());
                        return singleRecall(query.getDatabase(), query.getTable(), tempQuery, isReciprocalRankFusion);
                    });
                    responses.add(response);
                }

                if (query.vectorField().isPresent()) {
                    var response = executors.submit(() -> {
                        var tempQuery = new SearchQuery(query.getDatabase(), query.getTable(), Optional.empty(), query.fields(), query.vector(), query.vectorField(), query.limit());
                        return singleRecall(query.getDatabase(), query.getTable(), tempQuery, isReciprocalRankFusion);
                    });
                    responses.add(response);
                }
            }
            for (var response : responses) {
                scoreResults.add(response.get(30, TimeUnit.SECONDS));
            }
        }

        // merge the ScoreResult
        Map<Object, Float> newScores = new HashMap<>();
        Map<Object, Map<String, Object>> idToDocs = new HashMap<>();

        for (var scoreResult : scoreResults) {
            idToDocs.putAll(scoreResult.getIdToDocs());
            for(var entry : scoreResult.getNewScores().entrySet()) {
                var id = entry.getKey();
                var score = entry.getValue();
                if (!newScores.containsKey(id)) {
                    newScores.put(id, 0.0f);
                }
                var previewScore = newScores.get(id);
                var updatedScore = previewScore + score;
                newScores.put(id, updatedScore);
            }
        }

        // convert the newScores to Entry list and sort by score descent
        var newScoresList = new ArrayList<Map.Entry<Object, Float>>(newScores.entrySet());
        newScoresList.sort((o1, o2) -> {
            var score1 = o1.getValue();
            var score2 = o2.getValue();
            return Float.compare(score2, score1);
        });

        // take query.limit items from newScoresList, make sure the query.limit is not bigger then the size of  newScoresList
        var limit = sampleQuery.limit();
        if (limit > newScoresList.size()) {
            limit = newScoresList.size();
        }
        var finalScoresList = newScoresList.subList(0, limit);

        var jsonResult = new ArrayList<Map<String, Object>>();
        for (var item : finalScoresList) {
            var doc = idToDocs.get(item.getKey());
            doc.put("_score", item.getValue());
            jsonResult.add(doc);
        }
        return Utils.toJson(jsonResult);
    }

    // commit the target table index
    public boolean commit(String database, String table) throws Exception {
        var tasks = new ArrayList<ObjectRef<Long>>();
        for (var worker : this.workers) {
            var ref = worker.task(RetrievalWorker::commit, database, table).remote();
            tasks.add(ref);
        }
        Ray.get(tasks);
        return true;
    }

    public boolean truncate(String database, String table) throws Exception {
        var tasks = new ArrayList<ObjectRef<Boolean>>();
        for (var worker : this.workers) {
            var ref = worker.task(RetrievalWorker::truncate, database, table).remote();
            tasks.add(ref);
        }
        Ray.get(tasks);
        return true;
    }

    public boolean close(String database, String table) throws Exception {
        var tasks = new ArrayList<ObjectRef<Boolean>>();
        for (var worker : this.workers) {
            var ref = worker.task(RetrievalWorker::close, database, table).remote();
            tasks.add(ref);
        }
        Ray.get(tasks);
        var targetTableSettings = this.clusterInfo.findTableSettings(database, table);
        if(targetTableSettings.isPresent()) {
            targetTableSettings.get().setStatus("close");
        }
        return true;
    }

    public boolean closeAndDeleteFile(String database, String table) throws Exception {
        var tasks = new ArrayList<ObjectRef<Boolean>>();
        for (var worker : this.workers) {
            var ref = worker.task(RetrievalWorker::closeAndDeleteFile, database, table).remote();
            tasks.add(ref);
        }
        Ray.get(tasks);
        this.clusterInfo.removeTableSettings(database,table);
        return true;
    }

    public void shutdown() throws Exception {
        Ray.exitActor();
    }
}

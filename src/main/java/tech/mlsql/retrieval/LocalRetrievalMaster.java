package tech.mlsql.retrieval;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.mlsql.retrieval.records.*;
import tech.mlsql.retrieval.schema.SchemaUtils;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LocalRetrievalMaster {
    private static final Logger logger = LoggerFactory.getLogger(LocalRetrievalMaster.class);
    
    private List<RetrievalWorker> workers = new ArrayList<>();
    private ClusterInfo clusterInfo;

    public LocalRetrievalMaster(ClusterInfo clusterInfo) {
        logger.info("Initializing LocalRetrievalMaster with cluster settings: {}", clusterInfo.clusterSettings());
        for (int i = 0; i < clusterInfo.clusterSettings().getNumNodes(); i++) {
            logger.debug("Creating RetrievalWorker instance {}", i);
            workers.add(new RetrievalWorker(clusterInfo, i));
        }
        this.clusterInfo = clusterInfo;
        logger.info("LocalRetrievalMaster initialized with {} workers", workers.size());
    }

    public String clusterInfo() throws Exception {
        return Utils.toJson(clusterInfo);
    }

    public boolean createTable(String tableSettingStr) throws Exception {
        logger.info("Creating table with settings: {}", tableSettingStr);
        var tableSettings = Utils.toRecord(tableSettingStr, TableSettings.class);
        logger.debug("Parsed table settings: {}", tableSettings);
        
        for (var worker : workers) {
            logger.debug("Creating table on worker {}", workers.indexOf(worker));
            worker.createTable(tableSettings);
        }
        this.clusterInfo.tableSettingsList().add(tableSettings);
        logger.info("Table created successfully");
        return true;
    }

    public boolean build(String database, String table, String dataLocationStr) throws Exception {
        for (var worker : workers) {
            worker.build(database, table, dataLocationStr);
        }
        return true;
    }

    public boolean buildFromRayObjectStore(String database, String table, byte[][] batchData, byte[][] locations) throws Exception {
        throw new UnsupportedOperationException("Not supported in local mode");
    }

    public boolean buildFromLocal(String database, String table, List<String> batchData) throws Exception {
        logger.info("Building from local data for database: {}, table: {} with {} records", 
            database, table, batchData.size());
        for (var worker : workers) {
            logger.debug("Building data on worker {}", workers.indexOf(worker));
            worker.buildFromLocal(database, table, batchData);
        }
        logger.info("Build from local completed successfully");
        return true;
    }

    public String filter(String queryStr) throws Exception {
        List<SearchQuery> queries = Utils.toSearchQueryList(queryStr);
        List<SearchResult> collectedResults = new ArrayList<>();

        var sampleQuery = queries.get(0);
        try (var executors = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<List<SearchResult>>> responses = new ArrayList<>();
            for (var query : queries) {
                var response = executors.submit(() -> {
                    List<SearchResult> result = new ArrayList<>();
                    for (var worker : workers) {
                        result.addAll(worker.filter(query.getDatabase(), query.getTable(), Utils.toJson(query)));
                    }
                    return result;
                });
                responses.add(response);
            }
            for (var response : responses) {
                collectedResults.addAll(response.get(30, TimeUnit.SECONDS));
            }
        }
        
        var sorts = sampleQuery.getSorts();
        if (!sorts.isEmpty()) {
            Collections.sort(collectedResults, (o1, o2) -> {
                for (int i = 0; i < sorts.size(); i++) {
                    var sort = sorts.get(i);
                    var field = sort.keySet().iterator().next();
                    var order = sort.get(field);

                    var value1 = (Comparable) o1.doc().get(field);
                    var value2 = (Comparable) o2.doc().get(field);

                    int result = value1.compareTo(value2);
                    if (result != 0) {
                        if (order.equals("desc")) {
                            return -result;
                        } else {
                            return result;
                        }
                    }
                }
                return 0;
            });
        }
        return Utils.toJson(collectedResults.stream().map(f -> f.doc()).collect(Collectors.toList()));
    }

    public boolean deleteByFilter(String database, String table, String condition) throws Exception {
        for (var worker : workers) {
            worker.deleteByFilter(database, table, condition);
        }
        return true;
    }

    public boolean deleteByIds(String database, String table, String ids) throws Exception {
        List<Object> idList = Utils.toRecord(ids, List.class);
        var shardIdsMap = new HashMap<Integer, ArrayList<Object>>();
        for (var id : idList) {
            var shardId = Utils.route(id, workers.size());
            shardIdsMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(id);
        }
        for (int i = 0; i < workers.size(); i++) {
            var shardIds = shardIdsMap.get(i);
            if (shardIds == null || shardIds.isEmpty()) {
                continue;
            }
            workers.get(i).deleteByIds(database, table, Utils.toJson(shardIds));
        }
        return true;
    }

    public String search(String queryStr) throws Exception {
        logger.debug("Received search request: {}", queryStr);
        List<SearchQuery> queries = Utils.toSearchQueryList(queryStr);
        logger.debug("Parsed search queries: {}", queries);
        List<ScoreResult> scoreResults = new ArrayList<>();

        var sampleQuery = queries.get(0);

        try (var executors = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<ScoreResult>> responses = new ArrayList<>();
            for (var query : queries) {
                boolean isReciprocalRankFusion = query.keyword().isPresent() && query.vectorField().isPresent();

                if (query.keyword().isPresent()) {
                    var response = executors.submit(() -> {
                        var tempQuery = new SearchQuery(query.getDatabase(),
                                query.getTable(),
                                query.getFilters(),
                                query.getSorts(),
                                query.keyword(), query.fields(),
                                query.vector(), Optional.empty(),
                                query.limit());
                        return singleRecall(query.getDatabase(), query.getTable(), tempQuery, isReciprocalRankFusion);
                    });
                    responses.add(response);
                }

                if (query.vectorField().isPresent()) {
                    var response = executors.submit(() -> {
                        var tempQuery = new SearchQuery(query.getDatabase(), query.getTable(),
                                query.getFilters(), query.getSorts(),
                                Optional.empty(), query.fields(), query.vector(), query.vectorField(),
                                query.limit());
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
            for (var entry : scoreResult.getNewScores().entrySet()) {
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

        // take query.limit items from newScoresList
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
        String result = Utils.toJson(jsonResult);
        logger.debug("Search completed with {} results", jsonResult.size());
        return result;
    }

    private List<SearchResult> inner_search(String database, String table, SearchQuery searchQuery) throws Exception {
        List<SearchResult> result = new ArrayList<>();
        for (var worker : workers) {
            result.addAll(worker.search(database, table, Utils.toJson(searchQuery)));
        }
        result.sort((o1, o2) -> {
            var score1 = o1.score();
            var score2 = o2.score();
            return Float.compare(score2, score1);
        });
        return result;
    }

    private ScoreResult singleRecall(String database, String table, SearchQuery tempQuery, boolean isReciprocalRankFusion) throws Exception {
        Map<Object, Float> newScores = new HashMap<>();
        Map<Object, Map<String, Object>> idToDocs = new HashMap<>();

        List<SearchResult> result = inner_search(database, table, tempQuery);
        if (isReciprocalRankFusion) {
            for (int i = 0; i < result.size(); i++) {
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

    public boolean commit(String database, String table) throws Exception {
        for (var worker : workers) {
            worker.commit(database, table);
        }
        return true;
    }

    public boolean truncate(String database, String table) throws Exception {
        for (var worker : workers) {
            worker.truncate(database, table);
        }
        return true;
    }

    public boolean close(String database, String table) throws Exception {
        for (var worker : workers) {
            worker.close(database, table);
        }
        var targetTableSettings = this.clusterInfo.findTableSettings(database, table);
        if (targetTableSettings.isPresent()) {
            targetTableSettings.get().setStatus("close");
        }
        return true;
    }

    public boolean closeAndDeleteFile(String database, String table) throws Exception {
        logger.info("Closing and deleting file for database: {}, table: {}", database, table);
        for (var worker : workers) {
            logger.debug("Closing and deleting file on worker {}", workers.indexOf(worker));
            worker.closeAndDeleteFile(database, table);
        }
        this.clusterInfo.removeTableSettings(database, table);
        logger.info("Close and delete file completed successfully");
        return true;
    }

    public void shutdown() throws Exception {
        // No need to do anything special for local mode
    }
}
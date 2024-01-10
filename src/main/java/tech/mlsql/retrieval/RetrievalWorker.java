package tech.mlsql.retrieval;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.NIOFSDirectory;
import tech.mlsql.retrieval.batchserver.BatchServer;
import tech.mlsql.retrieval.records.*;
import tech.mlsql.retrieval.schema.SchemaUtils;
import tech.mlsql.retrieval.schema.SimpleSchemaParser;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * 10/6/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class RetrievalWorker {
    private ClusterInfo clusterInfo;
    private List<Searcher> tableSeacherList;

    private int workerId;

    private BatchServer batchServer;

    public RetrievalWorker(ClusterInfo clusterInfo, int workerId) {
        this.clusterInfo = clusterInfo;
        this.tableSeacherList = new ArrayList<>();
        this.workerId = workerId;
        for (var tableSettings : clusterInfo.getTableSettingsList()) {
            try {
                createTable(tableSettings);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//        this.batchServer = new BatchServer(this, getNode(), 0);
//        this.batchServer.start();
    }

    public int getWorkerId() {
        return this.workerId;
    }

    public String getNode() {
        var nodes = Ray.getRuntimeContext().getAllNodeInfo();
        var currentNodeId = Ray.getRuntimeContext().getCurrentNodeId();
        var currentNode = nodes.stream().filter(f -> f.nodeId.equals(currentNodeId)).findFirst().get();
        return currentNode.nodeAddress;
    }


    public boolean createTable(TableSettings tableSettings) throws Exception {

        IndexWriterConfig writerConfig = new IndexWriterConfig(new WhitespaceAnalyzer());

        // with the worker id, we can use shared file system
        // to avoid different worker write to the same index location
        Path indexLocation = Paths.get(clusterInfo.getClusterSettings().location(),
                tableSettings.database(),
                tableSettings.table(),
                String.valueOf(workerId));

        NIOFSDirectory niofsDirectory = new NIOFSDirectory(indexLocation);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter indexWriter = new IndexWriter(niofsDirectory, writerConfig);
        SearcherManager manager = new SearcherManager(indexWriter, true, false, new SearcherFactory());

        // start a thread to refresh the searcher every second
        final ControlledRealTimeReopenThread<IndexSearcher> nrtReopenThread =
                new ControlledRealTimeReopenThread<>(indexWriter, manager, 1, 0.1);
        nrtReopenThread.setName(tableSettings.database() + " " + tableSettings.table() + " NRT Reopen Thread");
        nrtReopenThread.setPriority(Math.min(Thread.currentThread().getPriority() + 2, Thread.MAX_PRIORITY));
        nrtReopenThread.setDaemon(true);
        nrtReopenThread.start();

        tableSeacherList.add(new Searcher(tableSettings, indexWriter, manager));

        return true;
    }

    private Searcher getSearcher(String database, String table) {
        return this.tableSeacherList.stream().filter(f ->
                        f.tableSettings().database().equals(database) &&
                                f.tableSettings().table().equals(table)).findFirst()
                .get();
    }


    public boolean deleteByIds(String database, String table, String ids) throws Exception {
        List<Object> id_list = Utils.toRecord(ids, List.class);
        var searcher = getSearcher(database, table);
        var tableSettings = searcher.tableSettings();

        var schema = SchemaUtils.getSchema(tableSettings.schema());
        var id_field = schema.fields().stream().filter(f -> f.name().equals("_id")).findFirst().orElseThrow(() -> new RuntimeException("Can not find _id field"));


        try {
            var queries = new Query[id_list.size()];
            for (int i = 0; i < id_list.size(); i++) {
                queries[i] = SchemaUtils.toLuceneQuery(id_field, id_list.get(i), null);
            }
            searcher.indexWriter().deleteDocuments(queries);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            // Utils.writeExceptionToFile(e);
            return false;
        }
    }


    public Iterator<Document> iterateAllDocs(String database, String table) throws IOException {
        var searcher = getSearcher(database, table).searcherManager();
        final IndexSearcher indexSearcher = searcher.acquire();
        final IndexReader indexReader = indexSearcher.getIndexReader();
        AtomicInteger i = new AtomicInteger(0);
        return new Iterator<Document>() {

            @Override
            public boolean hasNext() {
                return i.get() < indexReader.maxDoc();
            }

            @Override
            public Document next() {
                Document doc = null;
                try {
                    doc = indexReader.document(i.getAndIncrement());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return doc;

            }
        };

    }

    public boolean build(String database, String table, String dataLocationStr) throws Exception {
        var searcher = getSearcher(database, table);
        var tableSettings = searcher.tableSettings();
        var schema = new SimpleSchemaParser().parse(tableSettings.schema());
        var indexWriter = searcher.indexWriter();
        var dataLocation = Paths.get(dataLocationStr);
        if (Files.isDirectory(dataLocation)) {
            Files.walkFileTree(dataLocation, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    // file is jsonl format , read line by line and parse with jackson
                    try (var reader = Files.newBufferedReader(file)) {
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            Map<String, Object> data = Utils.toRecord(line, Map.class);
                            Document doc = new Document();
                            for (var field : schema.fields()) {
                                var value = data.get(field.name());
                                if (value == null) {
                                    continue;
                                }
                                for (var v : SchemaUtils.toLuceneField(field, value)) {
                                    doc.add(v);
                                }

                            }
                            indexWriter.addDocument(doc);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                }
            });
        }

        return true;
    }

    public boolean buildFromRayObjectStore(String database, String table, List<ObjectRef<String>> batchData) throws Exception {
        var searcher = getSearcher(database, table);
        var tableSettings = searcher.tableSettings();

        var schema = SchemaUtils.getSchema(tableSettings.schema());

        var indexWriter = searcher.indexWriter();
        for (var dataRef : batchData) {
            Document doc = new Document();
            var data = Utils.toRecord(Ray.get(dataRef), Map.class);
            for (var field : schema.fields()) {
                var value = data.get(field.name());
                if (value == null) {
                    continue;
                }
                for (var v : SchemaUtils.toLuceneField(field, value)) {
                    doc.add(v);
                }
            }
            if (doc.getField("_id") instanceof LongField) {
                LongField longField = (LongField) doc.getField("_id");
                indexWriter.updateDocuments(LongField.newExactQuery("_id", (Long) longField.numericValue()), List.of(doc));
            } else {
                indexWriter.updateDocument(new Term("_id", data.get("_id").toString()), doc);
            }

        }
        return true;
    }


    public List<SearchResult> filter(String database, String table, String queryStr) throws Exception {
        var searcher = getSearcher(database, table);
        var query = Utils.toRecord(queryStr, SearchQuery.class);
        var searcherManager = searcher.searcherManager();
        final IndexSearcher indexSearcher = searcherManager.acquire();

        var sampleQuery = query;
        var tableSettings = getSearcher(database, table).tableSettings();
        var sort = Utils.buidSort(sampleQuery, tableSettings);

        List<SearchResult> result = new ArrayList<>();
        try {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            Utils.buildFilter(builder, query, searcher);

            var finalQuery = builder.build();
            TopDocs docs = null;
            if (sort.isEmpty()) {
                docs = indexSearcher.search(finalQuery, query.limit());
            } else {
                docs = indexSearcher.search(finalQuery, query.limit(), sort.get());
            }
            for (ScoreDoc scoreDoc : docs.scoreDocs) {
                var doc = Utils.documentToMap(indexSearcher.doc(scoreDoc.doc));
                result.add(new SearchResult(scoreDoc.score, doc));
            }
        } finally {
            if (indexSearcher != null) {
                searcherManager.release(indexSearcher);
            }
        }

        return result;

    }

    public List<SearchResult> search(String database, String table, String queryStr) throws
            Exception {

        var searcher = getSearcher(database, table);
        var query = Utils.toRecord(queryStr, SearchQuery.class);
        var searcherManager = searcher.searcherManager();
        final IndexSearcher indexSearcher = searcherManager.acquire();

        List<SearchResult> result = new ArrayList<>();

        try {
            // convert fields to map<String,Float>
            var queryFields = query.fields().stream().map(f -> Map.entry(f, 1.0f)).
                    collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));


            BooleanQuery.Builder builder = new BooleanQuery.Builder();

            // Actually the keyword and vector field can not be both present
            // the RetrievalMaster will check this
            assert (query.keyword().isPresent() || query.vectorField().isPresent());

            Utils.buildFilter(builder, query, searcher);

            Query finalQuery = null;

            if (query.keyword().isPresent()) {
                if (query.keyword().get().trim().equals("*")) {
                    builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
                } else {
                    Query parsedQuery = new SimpleQueryParser(new WhitespaceAnalyzer(), queryFields).
                            parse(query.keyword().get());
                    builder.add(parsedQuery, BooleanClause.Occur.SHOULD);
                }
                finalQuery = builder.build();
            }
            
            if (query.vectorField().isPresent()) {

                var tempFilter = builder.build();
                Query targetFilter = null;
                if (tempFilter.clauses().size() > 0) {
                    targetFilter = tempFilter.clauses().get(0).getQuery();
                }

                if (targetFilter != null) {
                    finalQuery = new KnnFloatVectorQuery(
                            query.vectorField().get(),
                            query.vector(),
                            query.limit(),
                            targetFilter);
                } else {
                    finalQuery = new KnnFloatVectorQuery(
                            query.vectorField().get(),
                            query.vector(),
                            query.limit());
                }
            }

            TopDocs docs = indexSearcher.search(finalQuery, query.limit());
            for (ScoreDoc scoreDoc : docs.scoreDocs) {
                var doc = Utils.documentToMap(indexSearcher.doc(scoreDoc.doc));
                result.add(new SearchResult(scoreDoc.score, doc));
            }

        } finally {
            if (indexSearcher != null) {
                searcherManager.release(indexSearcher);
            }
        }
        return result;
    }

    public long commit(String database, String table) throws Exception {
        var searcher = getSearcher(database, table);
        try {
            var v = searcher.indexWriter().commit();
            searcher.searcherManager().maybeRefresh();
            return v;
        } catch (Exception e) {
            e.printStackTrace();
            // Utils.writeExceptionToFile(e);
            return -1;
        }
    }

    public boolean truncate(String database, String table) throws Exception {
        var searcher = getSearcher(database, table);
        try {
            searcher.indexWriter().deleteAll();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            // Utils.writeExceptionToFile(e);
            return false;
        }

    }

    public boolean close(String database, String table) throws Exception {
        var searcher = getSearcher(database, table);
        try {
            searcher.searcherManager().close();
            searcher.indexWriter().close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            // Utils.writeExceptionToFile(e);
            return false;
        }
    }

    public boolean closeAndDeleteFile(String database, String table) throws Exception {
        close(database, table);
        FileUtils.deleteDirectory(Paths.get(clusterInfo.getClusterSettings().location(),
                database,
                table,
                String.valueOf(workerId)).toFile());
        return true;
    }
}

package tech.mlsql.retrieval;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.NIOFSDirectory;
import tech.mlsql.retrieval.records.*;
import tech.mlsql.retrieval.schema.SimpleSchemaParser;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;


import static java.lang.StringTemplate.STR;

/**
 * 10/6/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class RetrievalWorker {
    private ClusterSettings clusterSettings;
    private List<Searcher> tableSeacherList;

    public RetrievalWorker(ClusterSettings clusterSettings, TableSettings tableSettings) {
        this.clusterSettings = clusterSettings;
        this.tableSeacherList = new ArrayList<>();
        try {
            this.createTable(tableSettings);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean createTable(TableSettings tableSettings) throws Exception {

        IndexWriterConfig writerConfig = new IndexWriterConfig(new WhitespaceAnalyzer());

        Path indexLocation = Paths.get(clusterSettings.location(), tableSettings.database(), tableSettings.table());
        NIOFSDirectory niofsDirectory = new NIOFSDirectory(indexLocation);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter indexWriter = new IndexWriter(niofsDirectory, writerConfig);
        SearcherManager manager = new SearcherManager(indexWriter, false, false, new SearcherFactory());

        // start a thread to refresh the searcher every second
        final ControlledRealTimeReopenThread<IndexSearcher> nrtReopenThread =
                new ControlledRealTimeReopenThread<>(indexWriter, manager, 1, 1);
        nrtReopenThread.setName(STR. "\{ tableSettings.database() }.\{ tableSettings.table() } NRT Reopen Thread" );
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
                                doc.add(SimpleSchemaParser.toLuceneField(field, value));
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

        var schema = new SimpleSchemaParser().parse(tableSettings.schema());

        var indexWriter = searcher.indexWriter();
        for (var dataRef : batchData) {
            Document doc = new Document();
            var data = Utils.toRecord(Ray.get(dataRef), Map.class);
            for (var field : schema.fields()) {
                var value = data.get(field.name());
                if (value == null) {
                    continue;
                }
                doc.add(SimpleSchemaParser.toLuceneField(field, value));
            }
            indexWriter.addDocument(doc);
        }
        return true;
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
            var queryFields = query.fields().stream().map(f -> Map.entry(f, 1.0f)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));


            BooleanQuery.Builder builder = new BooleanQuery.Builder();

            assert (query.keyword().isPresent() || query.vectorField().isPresent());

            if (query.keyword().isPresent()) {
                Query parsedQuery = new SimpleQueryParser(new WhitespaceAnalyzer(), queryFields).
                        parse(query.keyword().get());
                builder.add(parsedQuery, BooleanClause.Occur.SHOULD);
            }

            if (query.vectorField().isPresent()) {
                KnnFloatVectorQuery knnQuery =
                        new KnnFloatVectorQuery(
                                query.vectorField().get(),
                                query.vector(),
                                query.limit());
                builder.add(knnQuery, BooleanClause.Occur.SHOULD);
            }

            var finalQuery = builder.build();

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
        var searcher = getSearcher(database,table);
        return searcher.indexWriter().commit();
    }
}

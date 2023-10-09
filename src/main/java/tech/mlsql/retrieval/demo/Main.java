package tech.mlsql.retrieval.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import tech.mlsql.retrieval.RetrievalWorker;
import tech.mlsql.retrieval.Utils;
import tech.mlsql.retrieval.records.ClusterSettings;
import tech.mlsql.retrieval.records.SearchQuery;
import tech.mlsql.retrieval.records.TableSettings;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 10/4/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class Main {
    public static void main(String[] args) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        var data = new ArrayList<Map<String, Object>>();
        data.add(Map.of("_id", 1, "name", "a", "content", "b c", "vector", new float[]{1.0f, 2.0f, 3.0f}));
        data.add(Map.of("_id", 2, "name", "d", "content", "b e", "vector", new float[]{1.0f, 2.6f, 4.0f}));
        var dataPath = Paths.get("/tmp/data");
        FileUtils.deleteDirectory(new File("/tmp/data"));
        if (!Files.exists(dataPath)) {
            Files.createDirectories(dataPath);
            try (var writer = Files.newBufferedWriter(Paths.get("/tmp/data/1.json"))) {
                for (var item : data) {
                    var json = mapper.writeValueAsString(item);
                    writer.write(json);
                    writer.write("\n");
                }
            }
        }

        FileUtils.deleteDirectory(new File("/tmp/cluster1"));
        var worker = new RetrievalWorker(
                new ClusterSettings("cluster1", "/tmp/cluster1"),
                new TableSettings("db1",
                        "table1",
                        "st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))",
                        null,
                        1)
        );
        worker.build("db1", "table1", "/tmp/data");

        var query = new SearchQuery(
                Optional.empty(),
                List.of("content"),
                new float[]{1.0f, 2.6f, 4.0f},
                Optional.of("vector"),
                10
        );
        Thread.sleep(5000);
        var result = worker.search("db1", "table1", Utils.toJson(query));
        for (var item : result) {
            System.out.println(item.doc().get("content"));
        }

        var jsonResult = new ArrayList<Map<String, Object>>();
        for (var item : result.stream().limit(query.limit()).collect(Collectors.toList())) {
            var doc = item.doc();
            doc.put("_score", item.score());
            jsonResult.add(doc);
        }

        System.out.println(Utils.toJson(jsonResult));
    }
}

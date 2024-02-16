package tech.mlsql.retrieval.test;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import tech.mlsql.retrieval.RetrievalWorker;
import tech.mlsql.retrieval.Utils;
import tech.mlsql.retrieval.records.*;
import tech.mlsql.retrieval.schema.SimpleSchemaParser;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 2/16/24 WilliamZhu(allwefantasy@gmail.com)
 */
public class LuceneKNNTest {
    @Test
    public void test() throws Exception {
        var settings = new ClusterSettings(
                "test","/tmp/cluster",1
        );

        var tempFile = new File("/tmp/cluster");
        if (tempFile.exists()) {
            FileUtils.deleteDirectory(tempFile);
        }

        
        var  jvmSettings = new JVMSettings();
        jvmSettings.setOptions(List.of("-Xmx1g"));

        var envSettings = new EnvSettings();
        envSettings.setPath("/");
        envSettings.setJavaHome("/");

        var resourceRequirementSettings = new ResourceRequirementSettings();
        resourceRequirementSettings.setResourceRequirements(List.of(
                new ResourceRequirement("cpu",1),
                new ResourceRequirement("memory",1024)
        ));

        var clusterInfo = new ClusterInfo(
                settings,jvmSettings,envSettings,resourceRequirementSettings
        );
        var worker = new RetrievalWorker(
               clusterInfo ,0
        );

        var tableSettings = new TableSettings();
        tableSettings.setDatabase("db1");
        tableSettings.setTable("table1");
        tableSettings.setSchema("st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))");
        tableSettings.setLocation("/tmp/cluster");
        tableSettings.setNum_shards(1);
        worker.createTable(tableSettings);


        // create a float array includes 1025 elements
        var v1 = new float[1025];
        for (int i = 0; i < 1025; i++) {
            v1[i] = 1.0f;
        }

        var v2 = new float[1025];
        for (int i = 0; i < 1025; i++) {
            v2[i] = 2.0f;
        }

        var data = List.of(
                Map.of("_id",1,"name","a","content","hello world","vector",v1),
                Map.of("_id",2,"name","b","content","hello world","vector",v2),
                Map.of("_id",3,"name","c","content","hello world","vector",v1)
        );
        // convert data to jsonl string and write to the file /tmp/data.json
        File baseDir = new File("/tmp/data");
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }
        File file = new File("/tmp/data/data.json");
        if (file.exists()) {
            file.delete();
        }
        for (var item : data) {
            FileUtils.write(file, Utils.toJson(item) + "\n", StandardCharsets.UTF_8, true);
        }
        worker.build("db1","table1","/tmp/data");
        var query = new SearchQuery(
                "db1",
                "table1",
                Map.of("and",List.of(Map.of("field","name","value","a"))),
                List.of(),
                Optional.empty(),
                List.of(),
                v1,
                Optional.of("vector"),
                10
        );
        worker.commit("db1","table1");
        var hits = worker.search("db1","table1",Utils.toJson(query));
        for (var hit : hits) {
            System.out.println(hit.getDoc());
        }
        assertEquals(hits.size(),1);
    }
}

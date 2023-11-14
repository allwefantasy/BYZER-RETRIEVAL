package tech.mlsql.retrieval.demo;

import io.ray.runtime.serializer.FstSerializer;
import tech.mlsql.retrieval.Utils;
import tech.mlsql.retrieval.records.ClusterSettings;
import tech.mlsql.retrieval.schema.SimpleSchemaParser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * 10/9/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class Test {
    public static void main(String[] args) throws Exception {
//
        for (int i = 0; i < 100; i++) {
            var v = UUID.randomUUID().toString();
            var m = Utils.route(v, 2);

            System.out.println(m+ "==="+ Utils.murmurhash3_x86_32(v));
        }

    }
}

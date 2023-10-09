package tech.mlsql.retrieval.demo;

import io.ray.runtime.serializer.FstSerializer;
import tech.mlsql.retrieval.Utils;
import tech.mlsql.retrieval.records.ClusterSettings;
import tech.mlsql.retrieval.schema.SimpleSchemaParser;

import java.util.ArrayList;
import java.util.List;

/**
 * 10/9/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class Test {
    public static void main(String[] args) throws Exception {
//
        var schema = new SimpleSchemaParser().parse("st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))");
        for(var field:schema.fields()){
            System.out.println(field.name());
        }

        

    }
}

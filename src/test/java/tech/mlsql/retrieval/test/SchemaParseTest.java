package tech.mlsql.retrieval.test;

import org.junit.jupiter.api.Test;
import tech.mlsql.retrieval.schema.SimpleSchemaParser;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 10/10/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class SchemaParseTest {
    @Test
    public void testSchemaParse(){

        var schema = new SimpleSchemaParser().parse("st(field(_id,long),field(name,string),field(content,string,analyze),field(vector,array(float)))");
        assertEquals(schema.fields().size(),4);
        for(var field:schema.fields()){
            System.out.println(field.name());
        }
    }
}

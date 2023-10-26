package tech.mlsql.retrieval.test;

import org.junit.jupiter.api.Test;
import tech.mlsql.retrieval.Utils;
import tech.mlsql.retrieval.records.SearchQuery;

import java.util.ArrayList;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 10/11/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class UtilsTest {
    @Test
    public void testRouteNumber(){
        var v = Utils.route(10l,3);
        assertEquals(v,1);
    }

    @Test
    public void testRouteString(){
        var v = Utils.route("hellowo",3);
        assertTrue(v >=0 && v < 3);
    }

    @Test
    public void testToRecordList() throws Exception{
        var searchQueries = new ArrayList<SearchQuery>();
        searchQueries.add(new SearchQuery(
                "db1",
                "table1",
                Optional.of("keyword"),
                new ArrayList<>(),
                new float[]{},
                Optional.of("vectorField"),
                10
        ));
        var json = Utils.toJson(searchQueries);
        System.out.println(json);
        var v = Utils.toSearchQueryList(json);
        assertEquals("db1",v.get(0).getDatabase());

    }
}

package tech.mlsql.retrieval.test;

import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.junit.jupiter.api.Test;
import tech.mlsql.retrieval.Utils;
import tech.mlsql.retrieval.records.SearchQuery;
import tech.mlsql.retrieval.records.Searcher;
import tech.mlsql.retrieval.records.TableSettings;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 10/11/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class UtilsTest {
    @Test
    public void testRouteNumber() {
        var v = Utils.route(10l, 3);
        assertEquals(v, 1);
    }

    @Test
    public void testRouteString() {
        var v = Utils.route("hellowo", 3);
        assertTrue(v >= 0 && v < 3);
    }

    @Test
    public void testToRecordList() throws Exception {
        var searchQueries = new ArrayList<SearchQuery>();
        var filters = new HashMap<String, Object>();
        var list = new ArrayList<Map<String, Object>>();
        list.add(Map.of("field", "col1","value","v1"));
        filters.put("and", list);
        searchQueries.add(new SearchQuery(
                "db1",
                "table1",
                filters,
                List.of(),
                Optional.of("keyword"),
                new ArrayList<>(),
                new float[]{},
                Optional.of("vectorField"),
                10
        ));
        var json = Utils.toJson(searchQueries);
        System.out.println(json);
        var v = Utils.toSearchQueryList(json);
        assertEquals("db1", v.get(0).getDatabase());

    }

    @Test
    public void testFilterBuilder() {
        var filters = new HashMap<String, Object>();
        var list = new ArrayList<Map<String, Object>>();
        list.add(Map.of("field", "col1","value","v1"));
        list.add(Map.of("or", List.of(Map.of("field", "col2","value","v2"))));
        filters.put("and", list);
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        var searchQuery = new SearchQuery();
        searchQuery.setFilters(filters);
        var table = new TableSettings();
        table.setSchema("st(field(col1,string),field(col2,string))");
        var searcher = new Searcher(table,null,null);
        Utils.buildFilter(builder,searchQuery,searcher);
        System.out.println(builder.build());

    }
}

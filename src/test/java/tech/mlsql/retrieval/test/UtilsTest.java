package tech.mlsql.retrieval.test;

import org.junit.jupiter.api.Test;
import tech.mlsql.retrieval.Utils;

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
}

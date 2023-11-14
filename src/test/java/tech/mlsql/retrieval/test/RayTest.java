package tech.mlsql.retrieval.test;

import io.ray.api.Ray;

/**
 * 11/10/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class RayTest {
    public static void main(String[] args) {
        Ray.init();
        var v = Ray.task(RayTest::test).remote().get();
        System.out.println(v);
    }

    public static String test() {
        return "hello";
    }
}

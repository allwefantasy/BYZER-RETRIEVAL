package tech.mlsql.retrieval.records;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record Doc(String id,String content) {
//}

public class Doc {
    private final String id;
    private final String content;

    public Doc(String id, String content) {
        this.id = id;
        this.content = content;
    }

    public String id() {
        return id;
    }

    public String content() {
        return content;
    }
}

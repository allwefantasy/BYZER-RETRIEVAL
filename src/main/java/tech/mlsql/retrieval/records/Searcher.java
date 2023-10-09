package tech.mlsql.retrieval.records;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.SearcherManager;

/**
 * 10/8/23 WilliamZhu(allwefantasy@gmail.com)
 */
//public record Searcher(TableSettings tableSettings,IndexWriter indexWriter, SearcherManager searcherManager) {
//}

public class Searcher {
    private final TableSettings tableSettings;
    private final IndexWriter indexWriter;
    private final SearcherManager searcherManager;

    public Searcher(TableSettings tableSettings, IndexWriter indexWriter, SearcherManager searcherManager) {
        this.tableSettings = tableSettings;
        this.indexWriter = indexWriter;
        this.searcherManager = searcherManager;
    }

    public TableSettings tableSettings() {
        return tableSettings;
    }

    public IndexWriter indexWriter() {
        return indexWriter;
    }

    public SearcherManager searcherManager() {
        return searcherManager;
    }
}

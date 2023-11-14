package tech.mlsql.retrieval.batchserver;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * 11/9/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class LuceneDataset implements AutoCloseable {
    private final List<ArrowRecordBatch> batches;
    private final Schema schema;
    private final long rows;

    public LuceneDataset(List<ArrowRecordBatch> batches, Schema schema, long rows) {
        this.batches = batches;
        this.schema = schema;
        this.rows = rows;
    }

    public List<ArrowRecordBatch> getBatches() {
        return batches;
    }

    public Schema getSchema() {
        return schema;
    }

    public long getRows() {
        return rows;
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(batches);
    }
}
package tech.mlsql.retrieval.demo.server;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

class Dataset implements AutoCloseable {
    private final List<ArrowRecordBatch> batches;
    private final Schema schema;
    private final long rows;

    public Dataset(List<ArrowRecordBatch> batches, Schema schema, long rows) {
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

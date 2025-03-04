package tech.mlsql.retrieval;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.mlsql.retrieval.records.SearchResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RetrievalFlightServer {
    private final LocalRetrievalMaster master;

    public RetrievalFlightServer(LocalRetrievalMaster master) {
        this.master = master;
    }

    public void start() throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
            FlightServer flightServer = FlightServer.builder(
                    allocator, 
                    location,
                    new RetrievalFlightProducer(master, allocator)
            ).build();
            
            flightServer.start();
            System.out.println("Arrow Flight Server started on port 33333");
            flightServer.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class RetrievalFlightProducer extends NoOpFlightProducer {
        private final LocalRetrievalMaster master;
        private final BufferAllocator allocator;

        public RetrievalFlightProducer(LocalRetrievalMaster master, BufferAllocator allocator) {
            this.master = master;
            this.allocator = allocator;
        }

        @Override
        public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
            try {
                if (action.getType().equals("CreateTable")) {
                    String json = new String(action.getBody(), StandardCharsets.UTF_8);
                    boolean success = master.createTable(json);
                    listener.onNext(new Result(Boolean.toString(success).getBytes(StandardCharsets.UTF_8)));
                    listener.onCompleted();
                }
            } catch (Exception e) {
                listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
            }
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            try {
                String queryJson = new String(ticket.getBytes(), StandardCharsets.UTF_8);
                List<SearchResult> results = master.search(queryJson);

                // Create schema
                Schema schema = new Schema(List.of(
                    new Field("id", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()), null),
                    new Field("score", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)), null)
                ));

                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    listener.start(root);
                    
                    VarCharVector idVector = (VarCharVector) root.getVector("id");
                    FloatVector scoreVector = (FloatVector) root.getVector("score");

                    // Fill data
                    for (SearchResult result : results) {
                        idVector.setSafe(root.getRowCount(), result.doc().get("_id").toString().getBytes(StandardCharsets.UTF_8));
                        scoreVector.setSafe(root.getRowCount(), result.score());
                        root.setRowCount(root.getRowCount() + 1);
                    }

                    listener.putNext();
                    listener.completed();
                }
            } catch (Exception e) {
                listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        LocalRetrievalMaster master = new LocalRetrievalMaster(...); // 初始化你的LocalRetrievalMaster
        new RetrievalFlightServer(master).start();
    }
}
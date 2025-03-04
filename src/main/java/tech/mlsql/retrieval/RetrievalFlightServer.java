package tech.mlsql.retrieval;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.Float4Vector;
import tech.mlsql.retrieval.records.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

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
                switch (action.getType()) {
                    case "CreateTable":
                        String json = new String(action.getBody(), StandardCharsets.UTF_8);
                        boolean success = master.createTable(json);
                        listener.onNext(new Result(Boolean.toString(success).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    case "DeleteByFilter":
                        String[] params = new String(action.getBody(), StandardCharsets.UTF_8).split("\n");
                        boolean deleteSuccess = master.deleteByFilter(params[0], params[1], params[2]);
                        listener.onNext(new Result(Boolean.toString(deleteSuccess).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    case "DeleteByIds":
                        String[] idsParams = new String(action.getBody(), StandardCharsets.UTF_8).split("\n");
                        boolean deleteIdsSuccess = master.deleteByIds(idsParams[0], idsParams[1], idsParams[2]);
                        listener.onNext(new Result(Boolean.toString(deleteIdsSuccess).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    case "Commit":
                        String[] commitParams = new String(action.getBody(), StandardCharsets.UTF_8).split("\n");
                        boolean commitSuccess = master.commit(commitParams[0], commitParams[1]);
                        listener.onNext(new Result(Boolean.toString(commitSuccess).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    case "Truncate":
                        String[] truncateParams = new String(action.getBody(), StandardCharsets.UTF_8).split("\n");
                        boolean truncateSuccess = master.truncate(truncateParams[0], truncateParams[1]);
                        listener.onNext(new Result(Boolean.toString(truncateSuccess).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    case "Close":
                        String[] closeParams = new String(action.getBody(), StandardCharsets.UTF_8).split("\n");
                        boolean closeSuccess = master.close(closeParams[0], closeParams[1]);
                        listener.onNext(new Result(Boolean.toString(closeSuccess).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    case "CloseAndDeleteFile":
                        String[] deleteParams = new String(action.getBody(), StandardCharsets.UTF_8).split("\n");
                        boolean deleteFileSuccess = master.closeAndDeleteFile(deleteParams[0], deleteParams[1]);
                        listener.onNext(new Result(Boolean.toString(deleteFileSuccess).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    case "Build":
                        String[] buildParams = new String(action.getBody(), StandardCharsets.UTF_8).split("\n");
                        boolean buildSuccess = master.build(buildParams[0], buildParams[1], buildParams[2]);
                        listener.onNext(new Result(Boolean.toString(buildSuccess).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                        
                    case "BuildFromLocal":
                        String[] localBuildParams = new String(action.getBody(), StandardCharsets.UTF_8).split("\n");
                        List<String> batchDataList = Arrays.asList(localBuildParams[2].split("\u0000"));
                        boolean localBuildSuccess = master.buildFromLocal(localBuildParams[0], localBuildParams[1], batchDataList);
                        listener.onNext(new Result(Boolean.toString(localBuildSuccess).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    case "Shutdown":
                        master.shutdown();
                        listener.onNext(new Result("true".getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                        break;
                    default:
                        listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("Unknown action type").toRuntimeException());
                }
            } catch (Exception e) {
                listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
            }
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            try {
                String queryJson = new String(ticket.getBytes(), StandardCharsets.UTF_8);
                String jsonResults = master.search(queryJson);
                List<SearchResult> results = Utils.fromJsonToSearchResults(jsonResults);

                // Create schema
                Schema schema = new Schema(List.of(
                    new Field("id", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()), null),
                    new Field("score", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)), null)
                ));

                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    listener.start(root);
                    
                    VarCharVector idVector = (VarCharVector) root.getVector("id");
                    Float4Vector scoreVector = (Float4Vector) root.getVector("score");

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
                listener.error(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
            }
        }

        @Override
        public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
            try {
                String clusterInfo = master.clusterInfo();
                Schema schema = new Schema(List.of(
                    new Field("info", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()), null)
                ));
                return new FlightInfo(schema, descriptor, List.of(new FlightEndpoint(new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)))), -1, 1);
            } catch (Exception e) {
                throw CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // Initialize cluster settings
        ClusterSettings clusterSettings = new ClusterSettings(
            "local",
                "/tmp/cluster",
                1
        );
        EnvSettings envSettings = new EnvSettings();
        JVMSettings jvmSettings = new JVMSettings(Utils.defaultJvmOptions());
        ResourceRequirementSettings resourceSettings = new ResourceRequirementSettings(
            Arrays.asList(new ResourceRequirement("", 0.1))
        );
        
        ClusterInfo clusterInfo = new ClusterInfo(
            clusterSettings,
            jvmSettings,
            envSettings,
            resourceSettings
        );
        
        LocalRetrievalMaster master = new LocalRetrievalMaster(clusterInfo);
        new RetrievalFlightServer(master).start();
    }
}
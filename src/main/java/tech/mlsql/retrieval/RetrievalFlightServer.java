package tech.mlsql.retrieval;

import com.google.flatbuffers.Table;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.Float4Vector;
import tech.mlsql.retrieval.records.*;

import java.io.ByteArrayInputStream;
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
        
        // Define Arrow schemas for different actions
        private static final Schema CREATE_TABLE_SCHEMA = new Schema(Arrays.asList(
            Field.nullable("database", Types.MinorType.VARCHAR.getType()),
            Field.nullable("table", Types.MinorType.VARCHAR.getType()),
            Field.nullable("schema", Types.MinorType.VARCHAR.getType()),
            Field.nullable("location", Types.MinorType.VARCHAR.getType()),
            Field.nullable("numShards", Types.MinorType.INT.getType())
        ));
        
        private static final Schema DELETE_BY_FILTER_SCHEMA = new Schema(Arrays.asList(
            Field.nullable("database", Types.MinorType.VARCHAR.getType()),
            Field.nullable("table", Types.MinorType.VARCHAR.getType()),
            Field.nullable("condition", Types.MinorType.VARCHAR.getType())
        ));
        
        private static final Schema BUILD_FROM_LOCAL_SCHEMA = new Schema(Arrays.asList(
            Field.nullable("database", Types.MinorType.VARCHAR.getType()),
            Field.nullable("table", Types.MinorType.VARCHAR.getType()),
            Field.nullable("data", new org.apache.arrow.vector.types.pojo.ArrowType.List(), 
                Field.nullable("data_element", Types.MinorType.VARCHAR.getType()))
        ));

        public RetrievalFlightProducer(LocalRetrievalMaster master, BufferAllocator allocator) {
            this.master = master;
            this.allocator = allocator;
        }

        @Override
        public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
            try (BufferAllocator allocator = new RootAllocator()) {
                switch (action.getType()) {
                    case "CreateTable": {
                        try (VectorSchemaRoot root = VectorSchemaRoot.create(CREATE_TABLE_SCHEMA, allocator)) {
                            ArrowStreamReader reader = new ArrowStreamReader(
                                new ByteArrayInputStream(action.getBody()), allocator);
                            reader.loadNextBatch();
                            root.setRowCount(reader.getVectorSchemaRoot().getRowCount());
                            
                            VarCharVector databaseVector = (VarCharVector) root.getVector("database");
                            VarCharVector tableVector = (VarCharVector) root.getVector("table");
                            VarCharVector schemaVector = (VarCharVector) root.getVector("schema");
                            VarCharVector locationVector = (VarCharVector) root.getVector("location");
                            IntVector numShardsVector = (IntVector) root.getVector("numShards");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));
                            String schema = new String(schemaVector.get(0));
                            String location = new String(locationVector.get(0));
                            int numShards = numShardsVector.get(0);
                            TableSettings tableSettings = new TableSettings(database,table,schema, location, numShards);
                            boolean success = master.createTable(Utils.toJson(tableSettings));
                            listener.onNext(new Result(Boolean.toString(success).getBytes()));
                        }
                        break;
                    }
                    case "DeleteByFilter": {
                        try (VectorSchemaRoot root = VectorSchemaRoot.create(DELETE_BY_FILTER_SCHEMA, allocator)) {
                            ArrowStreamReader reader = new ArrowStreamReader(
                                new ByteArrayInputStream(action.getBody()), allocator);
                            reader.loadNextBatch();
                            root.setRowCount(reader.getVectorSchemaRoot().getRowCount());
                            
                            VarCharVector databaseVector = (VarCharVector) root.getVector("database");
                            VarCharVector tableVector = (VarCharVector) root.getVector("table");
                            VarCharVector conditionVector = (VarCharVector) root.getVector("condition");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));
                            String condition = new String(conditionVector.get(0));

                            boolean deleteSuccess = master.deleteByFilter(database, table, condition);
                            listener.onNext(new Result(Boolean.toString(deleteSuccess).getBytes()));
                        }
                        break;
                    }
                    case "BuildFromLocal": {
                        try (VectorSchemaRoot root = VectorSchemaRoot.create(BUILD_FROM_LOCAL_SCHEMA, allocator)) {
                            ArrowStreamReader reader = new ArrowStreamReader(
                                new ByteArrayInputStream(action.getBody()), allocator);
                            reader.loadNextBatch();
                            root.setRowCount(reader.getVectorSchemaRoot().getRowCount());
                            
                            VarCharVector databaseVector = (VarCharVector) root.getVector("database");
                            VarCharVector tableVector = (VarCharVector) root.getVector("table");
                            ListVector dataVector = (ListVector) root.getVector("data");
                            VarCharVector dataElementsVector = (VarCharVector) dataVector.getDataVector();

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));
                            
                            List<String> batchDataList = new ArrayList<>();
                            int start = dataVector.getOffsetBuffer().getInt(0);
                            int end = dataVector.getOffsetBuffer().getInt(4);
                            for (int i = start; i < end; i++) {
                                batchDataList.add(new String(dataElementsVector.get(i)));
                            }

                            boolean localBuildSuccess = master.buildFromLocal(database, table, batchDataList);
                            listener.onNext(new Result(Boolean.toString(localBuildSuccess).getBytes()));
                        }
                        break;
                    }
                    case "Shutdown": {
                        master.shutdown();
                        listener.onNext(new Result("true".getBytes()));
                        break;
                    }
                    default:
                        listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("Unknown action type").toRuntimeException());
                }
                listener.onCompleted();
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
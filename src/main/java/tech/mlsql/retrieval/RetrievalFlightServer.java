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
import tech.mlsql.retrieval.schema.SchemaUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;


public class RetrievalFlightServer {
    private final LocalRetrievalMaster master;
    private final String host;
    private final int port;

    public RetrievalFlightServer(LocalRetrievalMaster master, String host, int port) {
        this.master = master;
        this.host = host;
        this.port = port;
    }

    public void start() throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            Location location = Location.forGrpcInsecure(host, port);
            FlightServer flightServer = FlightServer.builder(
                    allocator,
                    location,
                    new RetrievalFlightProducer(master, allocator)
            ).build();

            flightServer.start();            
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
                new Field("data",
                        FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.List()),
                        Collections.singletonList(
                                Field.notNullable("item", Types.MinorType.VARCHAR.getType())
                        )
                )
        ));
        
        private static final Schema SEARCH_SCHEMA = new Schema(Arrays.asList(
                Field.nullable("database", Types.MinorType.VARCHAR.getType()),
                Field.nullable("table", Types.MinorType.VARCHAR.getType()),
                Field.nullable("query", Types.MinorType.VARCHAR.getType())
        ));
        
        private static final Schema FILTER_SCHEMA = new Schema(Arrays.asList(
                Field.nullable("query", Types.MinorType.VARCHAR.getType())
        ));
        
        private static final Schema TRUNCATE_SCHEMA = new Schema(Arrays.asList(
                Field.nullable("database", Types.MinorType.VARCHAR.getType()),
                Field.nullable("table", Types.MinorType.VARCHAR.getType())
        ));
        
        private static final Schema COMMIT_SCHEMA = new Schema(Arrays.asList(
                Field.nullable("database", Types.MinorType.VARCHAR.getType()),
                Field.nullable("table", Types.MinorType.VARCHAR.getType())
        ));

        // Add a new schema definition for DeleteByIds
        private static final Schema DELETE_BY_IDS_SCHEMA = new Schema(Arrays.asList(
                Field.nullable("database", Types.MinorType.VARCHAR.getType()),
                Field.nullable("table", Types.MinorType.VARCHAR.getType()),
                Field.nullable("ids", Types.MinorType.VARCHAR.getType())  // JSON array of IDs
        ));

        public RetrievalFlightProducer(LocalRetrievalMaster master, BufferAllocator allocator) {
            this.master = master;
            this.allocator = allocator;
        }

        @Override
        public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
            try {
                switch (action.getType()) {
                    case "ClusterInfo": {
                        String clusterInfoJson = master.clusterInfo();
                        listener.onNext(new Result(clusterInfoJson.getBytes(StandardCharsets.UTF_8)));
                        break;
                    }
                    case "CreateTable": {
                        try (BufferAllocator localAllocator = allocator.newChildAllocator("create-table", 0, Long.MAX_VALUE);
                             ArrowStreamReader reader = new ArrowStreamReader(
                                     new ByteArrayInputStream(action.getBody()), localAllocator);
                             VectorSchemaRoot root = VectorSchemaRoot.create(CREATE_TABLE_SCHEMA, localAllocator)) {
                            
                            reader.loadNextBatch();
                            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
                            root.setRowCount(readerRoot.getRowCount());

                            VarCharVector databaseVector = (VarCharVector) readerRoot.getVector("database");
                            VarCharVector tableVector = (VarCharVector) readerRoot.getVector("table");
                            VarCharVector schemaVector = (VarCharVector) readerRoot.getVector("schema");
                            VarCharVector locationVector = (VarCharVector) readerRoot.getVector("location");
                            IntVector numShardsVector = (IntVector) readerRoot.getVector("numShards");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));
                            String schema = new String(schemaVector.get(0));
                            String location = new String(locationVector.get(0));
                            int numShards = numShardsVector.get(0);
                            TableSettings tableSettings = new TableSettings(database, table, schema, location, numShards);
                            boolean success = master.createTable(Utils.toJson(tableSettings));
                            listener.onNext(new Result(Boolean.toString(success).getBytes()));
                        }
                        break;
                    }
                    case "DeleteByFilter": {
                        try (BufferAllocator localAllocator = allocator.newChildAllocator("delete-filter", 0, Long.MAX_VALUE);
                             ArrowStreamReader reader = new ArrowStreamReader(
                                     new ByteArrayInputStream(action.getBody()), localAllocator);
                             VectorSchemaRoot root = VectorSchemaRoot.create(DELETE_BY_FILTER_SCHEMA, localAllocator)) {
                            
                            reader.loadNextBatch();
                            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
                            root.setRowCount(readerRoot.getRowCount());

                            VarCharVector databaseVector = (VarCharVector) readerRoot.getVector("database");
                            VarCharVector tableVector = (VarCharVector) readerRoot.getVector("table");
                            VarCharVector conditionVector = (VarCharVector) readerRoot.getVector("condition");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));
                            String condition = new String(conditionVector.get(0));

                            boolean deleteSuccess = master.deleteByFilter(database, table, condition);
                            listener.onNext(new Result(Boolean.toString(deleteSuccess).getBytes()));
                        }
                        break;
                    }
                    case "BuildFromLocal": {
                        try (BufferAllocator localAllocator = allocator.newChildAllocator("build-local", 0, Long.MAX_VALUE);
                             ArrowStreamReader reader = new ArrowStreamReader(
                                     new ByteArrayInputStream(action.getBody()), localAllocator);
                             VectorSchemaRoot root = VectorSchemaRoot.create(BUILD_FROM_LOCAL_SCHEMA, localAllocator)) {
                            
                            reader.loadNextBatch();
                            VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
                            
                            VarCharVector databaseVector = (VarCharVector) batchRoot.getVector("database");
                            VarCharVector tableVector = (VarCharVector) batchRoot.getVector("table");
                            org.apache.arrow.vector.complex.ListVector dataVector = 
                                (org.apache.arrow.vector.complex.ListVector) batchRoot.getVector("data");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));

                            // Get table settings to properly parse the JSON data
                            Optional<TableSettings> tableSettings = master.getClusterInfo().findTableSettings(database, table);
                            if (!tableSettings.isPresent()) {
                                throw new IllegalArgumentException("Table " + database + "." + table + " not found");
                            }

                            List<String> batchDataList = new ArrayList<>();
                            VarCharVector elementsVector = (VarCharVector) dataVector.getDataVector();
                            for (int i = 0; i < dataVector.getValueCount(); i++) {
                                int start = dataVector.getOffsetBuffer().getInt(i * 4);
                                int end = dataVector.getOffsetBuffer().getInt((i + 1) * 4);
                                for (int j = start; j < end; j++) {
                                    String jsonStr = new String(elementsVector.get(j));
                                    batchDataList.add(jsonStr);
                                }
                            }
                            try {
                                boolean localBuildSuccess = master.buildFromLocal(database, table, batchDataList);
                                listener.onNext(new Result(Boolean.toString(localBuildSuccess).getBytes()));
                            } catch (Exception e) {
                                e.printStackTrace();
                                listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
                            }
                        }
                        break;
                    }
                    case "Search": {
                        try (BufferAllocator localAllocator = allocator.newChildAllocator("search", 0, Long.MAX_VALUE);
                             ArrowStreamReader reader = new ArrowStreamReader(
                                     new ByteArrayInputStream(action.getBody()), localAllocator);
                             VectorSchemaRoot root = VectorSchemaRoot.create(SEARCH_SCHEMA, localAllocator)) {
                            
                            reader.loadNextBatch();
                            VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
                            
                            VarCharVector databaseVector = (VarCharVector) batchRoot.getVector("database");
                            VarCharVector tableVector = (VarCharVector) batchRoot.getVector("table");
                            VarCharVector queryVector = (VarCharVector) batchRoot.getVector("query");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));
                            String query = new String(queryVector.get(0));
                            // Convert back to JSON for the master
                            String searchResults = "[]";
                            try {
                                System.out.println(query);
                                searchResults = master.search(query);
                            }catch (Exception e) {
                                e.printStackTrace();
                                listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
                                return;
                            }
                            listener.onNext(new Result(searchResults.getBytes()));
                        }
                        break;
                    }
                    
                    case "Filter": {
                        try (BufferAllocator localAllocator = allocator.newChildAllocator("filter", 0, Long.MAX_VALUE);
                             ArrowStreamReader reader = new ArrowStreamReader(
                                     new ByteArrayInputStream(action.getBody()), localAllocator);
                             VectorSchemaRoot root = VectorSchemaRoot.create(FILTER_SCHEMA, localAllocator)) {
                            
                            reader.loadNextBatch();
                            VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
                            
                            VarCharVector queryVector = (VarCharVector) batchRoot.getVector("query");
                            String query = new String(queryVector.get(0));
                            
                            String filterResults = "[]";
                            try {
                                filterResults = master.filter(query);
                            } catch (Exception e) {
                                e.printStackTrace();
                                listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
                                return;
                            }
                            listener.onNext(new Result(filterResults.getBytes()));
                        }
                        break;
                    }
                    
                    case "Truncate": {
                        try (BufferAllocator localAllocator = allocator.newChildAllocator("truncate", 0, Long.MAX_VALUE);
                             ArrowStreamReader reader = new ArrowStreamReader(
                                     new ByteArrayInputStream(action.getBody()), localAllocator);
                             VectorSchemaRoot root = VectorSchemaRoot.create(TRUNCATE_SCHEMA, localAllocator)) {
                            
                            reader.loadNextBatch();
                            VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
                            
                            VarCharVector databaseVector = (VarCharVector) batchRoot.getVector("database");
                            VarCharVector tableVector = (VarCharVector) batchRoot.getVector("table");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));

                            boolean success = master.truncate(database, table);
                            listener.onNext(new Result(Boolean.toString(success).getBytes()));
                        }
                        break;
                    }
                    
                    case "Commit": {
                        try (BufferAllocator localAllocator = allocator.newChildAllocator("commit", 0, Long.MAX_VALUE);
                             ArrowStreamReader reader = new ArrowStreamReader(
                                     new ByteArrayInputStream(action.getBody()), localAllocator);
                             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(Arrays.asList(
                                     Field.nullable("database", Types.MinorType.VARCHAR.getType()),
                                     Field.nullable("table", Types.MinorType.VARCHAR.getType())
                             )), localAllocator)) {
                            
                            reader.loadNextBatch();
                            VectorSchemaRoot batchRoot = reader.getVectorSchemaRoot();
                            
                            VarCharVector databaseVector = (VarCharVector) batchRoot.getVector("database");
                            VarCharVector tableVector = (VarCharVector) batchRoot.getVector("table");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));

                            boolean success = master.commit(database, table);
                            listener.onNext(new Result(Boolean.toString(success).getBytes()));
                        }
                        break;
                    }
                    
                    case "DeleteByIds": {
                        try (BufferAllocator localAllocator = allocator.newChildAllocator("delete-ids", 0, Long.MAX_VALUE);
                             ArrowStreamReader reader = new ArrowStreamReader(
                                     new ByteArrayInputStream(action.getBody()), localAllocator);
                             VectorSchemaRoot root = VectorSchemaRoot.create(DELETE_BY_IDS_SCHEMA, localAllocator)) {
                            
                            reader.loadNextBatch();
                            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
                            root.setRowCount(readerRoot.getRowCount());

                            VarCharVector databaseVector = (VarCharVector) readerRoot.getVector("database");
                            VarCharVector tableVector = (VarCharVector) readerRoot.getVector("table");
                            VarCharVector idsVector = (VarCharVector) readerRoot.getVector("ids");

                            String database = new String(databaseVector.get(0));
                            String table = new String(tableVector.get(0));
                            String ids = new String(idsVector.get(0));

                            boolean deleteSuccess = master.deleteByIds(database, table, ids);
                            listener.onNext(new Result(Boolean.toString(deleteSuccess).getBytes()));
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
            BufferAllocator streamAllocator = null;
            try {
                streamAllocator = allocator.newChildAllocator("stream", 0, Long.MAX_VALUE);
                String queryJson = new String(ticket.getBytes(), StandardCharsets.UTF_8);
                String jsonResults = master.search(queryJson);
                List<SearchResult> results = Utils.fromJsonToSearchResults(jsonResults);

                // Create schema
                Schema schema = new Schema(List.of(
                        new Field("id", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()), null),
                        new Field("score", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(
                                org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)), null)
                ));

                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, streamAllocator)) {
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
            } finally {
                if (streamAllocator != null) {
                    streamAllocator.close();
                }
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
        // 解析命令行参数
        String configFile = null;
        String host = "127.0.0.1";
        int port = 33333;
        
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--file") && i + 1 < args.length) {
                configFile = args[i + 1];
                i++;
            } else if (args[i].equals("--host") && i + 1 < args.length) {
                host = args[i + 1];
                i++;
            } else if (args[i].equals("--port") && i + 1 < args.length) {
                try {
                    port = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.err.println("Error: Invalid port number. Using default port 33333.");
                }
                i++;
            }
        }

        ClusterInfo clusterInfo;
        
        // 如果指定了配置文件，从YAML文件加载配置
        if (configFile != null) {
            clusterInfo = loadClusterInfoFromYaml(configFile);            
        } else {
            // 使用默认配置
            // 使用 Path API 来确保跨平台兼容性
            Path defaultStoragePath = Paths.get(System.getProperty("user.home"), ".auto-coder", "storage", "local");
            
            ClusterSettings clusterSettings = new ClusterSettings(
                    "local",
                    defaultStoragePath.toString(),
                    1
            );
            EnvSettings envSettings = new EnvSettings();
            JVMSettings jvmSettings = new JVMSettings(Utils.defaultJvmOptions());
            ResourceRequirementSettings resourceSettings = new ResourceRequirementSettings(
                    Arrays.asList(new ResourceRequirement("", 0.1))
            );

            clusterInfo = new ClusterInfo(
                    clusterSettings,
                    jvmSettings,
                    envSettings,
                    resourceSettings
            );
        }

        // 确保存储目录存在 - 使用 Files API 处理目录创建
        Path storageDir = Paths.get(clusterInfo.clusterSettings().location());
        if (!Files.exists(storageDir)) {
            try {
                Files.createDirectories(storageDir);
                System.out.println("Created storage directory: " + storageDir.toAbsolutePath());
            } catch (IOException e) {
                System.err.println("ERROR: Failed to create storage directory: " + storageDir.toAbsolutePath());
                e.printStackTrace();
                throw new IOException("Cannot start server without a valid storage directory", e);
            }
        }

        LocalRetrievalMaster master = new LocalRetrievalMaster(clusterInfo);
        System.out.println("Starting Retrieval Flight Server on " + host + ":" + port);
        new RetrievalFlightServer(master, host, port).start();
    }
    
    /**
     * 从YAML文件加载集群配置
     * @param filePath YAML配置文件路径
     * @return 集群配置信息
     */
    private static ClusterInfo loadClusterInfoFromYaml(String filePath) throws IOException {
        try {
            Yaml yaml = new Yaml();
            Map<String, Object> config;
            
            try (FileInputStream inputStream = new FileInputStream(new File(filePath))) {
                config = yaml.load(inputStream);
            }

            // 解析集群设置
            Map<String, Object> clusterSettingsMap = (Map<String, Object>) config.getOrDefault("clusterSettings", Collections.emptyMap());
            String name = (String) clusterSettingsMap.getOrDefault("name", "local");
            
            // 跨平台路径处理
            String locationStr = (String) clusterSettingsMap.getOrDefault("location", null);
            Path location;
            
            if (locationStr == null) {
                // 使用默认路径
                location = Paths.get(System.getProperty("user.home"), ".auto-coder", "storage", "local");
            } else if (locationStr.startsWith("~")) {
                // 将 ~ 扩展为用户主目录
                location = Paths.get(System.getProperty("user.home"), locationStr.substring(1));
            } else {
                location = Paths.get(locationStr);
            }
            
            int numNodes = ((Number) clusterSettingsMap.getOrDefault("numNodes", 1)).intValue();
            ClusterSettings clusterSettings = new ClusterSettings(name, location.toString(), numNodes);
            
            // 解析环境设置
            Map<String, Object> envSettingsMap = (Map<String, Object>) config.getOrDefault("envSettings", Collections.emptyMap());
            String javaHome = (String) envSettingsMap.getOrDefault("javaHome", System.getenv("JAVA_HOME"));
            String path = (String) envSettingsMap.getOrDefault("path", System.getenv("PATH"));
            EnvSettings envSettings = new EnvSettings(javaHome, path);
            
            // 解析JVM设置
            Map<String, Object> jvmSettingsMap = (Map<String, Object>) config.getOrDefault("jvmSettings", Collections.emptyMap());
            List<String> options = (List<String>) jvmSettingsMap.getOrDefault("options", Utils.defaultJvmOptions());
            JVMSettings jvmSettings = new JVMSettings(options);
            
            // 解析资源需求设置
            Map<String, Object> resourceSettingsMap = (Map<String, Object>) config.getOrDefault("resourceSettings", Collections.emptyMap());
            List<Map<String, Object>> requirementsList = (List<Map<String, Object>>) resourceSettingsMap.getOrDefault("requirements", 
                    Collections.singletonList(Map.of("name", "", "quantity", 0.1)));
            
            List<ResourceRequirement> requirements = new ArrayList<>();
            for (Map<String, Object> req : requirementsList) {
                String reqName = (String) req.getOrDefault("name", "");
                double quantity = ((Number) req.getOrDefault("quantity", 0.1)).doubleValue();
                requirements.add(new ResourceRequirement(reqName, quantity));
            }
            
            ResourceRequirementSettings resourceSettings = new ResourceRequirementSettings(requirements);
            
            return new ClusterInfo(clusterSettings, jvmSettings, envSettings, resourceSettings);
        } catch (Exception e) {
            System.err.println("Error loading configuration from YAML file: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("Failed to load configuration from " + filePath, e);
        }
    }
}
package tech.mlsql.retrieval.demo.server;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 11/9/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class Client {
    public static void main(String[] args) {
        Location location = Location.forGrpcInsecure("127.0.0.1", 33333);
        // Client
        try (BufferAllocator allocator = new RootAllocator()){
        try (FlightClient flightClient = FlightClient.builder(allocator, location).build()) {
            System.out.println("C1: Client (Location): Connected to " + location.getUri());

            // Populate data
            Schema schema = new Schema(Arrays.asList(
                    new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
            try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
                VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("name")) {
                varCharVector.allocateNew(3);
                varCharVector.set(0, "Ronald".getBytes());
                varCharVector.set(1, "David".getBytes());
                varCharVector.set(2, "Francisco".getBytes());
                vectorSchemaRoot.setRowCount(3);
                FlightClient.ClientStreamListener listener = flightClient.startPut(
                        FlightDescriptor.path("profiles"),
                        vectorSchemaRoot, new AsyncPutListener());
                listener.putNext();
                varCharVector.set(0, "Manuel".getBytes());
                varCharVector.set(1, "Felipe".getBytes());
                varCharVector.set(2, "JJ".getBytes());
                vectorSchemaRoot.setRowCount(3);
                listener.putNext();
                listener.completed();
                listener.getResult();
                System.out.println("C2: Client (Populate Data): Wrote 2 batches with 3 rows each");
            }

            // Get metadata information
            FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
            System.out.println("C3: Client (Get Metadata): " + flightInfo);

            // Get data information
            try(FlightStream flightStream = flightClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
                int batch = 0;
                try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                    System.out.println("C4: Client (Get Stream):");
                    while (flightStream.next()) {
                        batch++;
                        System.out.println("Client Received batch #" + batch + ", Data:");
                        System.out.print(vectorSchemaRootReceived.contentToTSVString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Get all metadata information
            Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
            System.out.print("C5: Client (List Flights Info): ");
            flightInfosBefore.forEach(t -> System.out.println(t));

            // Do delete action
            Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE",
                    FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)));
            while (deleteActionResult.hasNext()) {
                Result result = deleteActionResult.next();
                System.out.println("C6: Client (Do Delete Action): " +
                        new String(result.getBody(), StandardCharsets.UTF_8));
            }

            // Get all metadata information (to validate detele action)
            Iterable<FlightInfo> flightInfos = flightClient.listFlights(Criteria.ALL);
            flightInfos.forEach(t -> System.out.println(t));
            System.out.println("C7: Client (List Flights Info): After delete - No records");
            
            System.out.println("C8: Server shut down successfully");
        }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

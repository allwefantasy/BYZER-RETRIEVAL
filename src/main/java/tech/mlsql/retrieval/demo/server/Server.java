package tech.mlsql.retrieval.demo.server;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;

/**
 * 11/9/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class Server {
    public static void main(String[] args) {
        Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
        try (BufferAllocator allocator = new RootAllocator()) {
            // Server
            try (final CookbookProducer producer = new CookbookProducer(allocator, location); final FlightServer flightServer = FlightServer.builder(allocator, location, producer).build()) {
                try {
                    flightServer.start();
                    System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                    flightServer.awaitTermination();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

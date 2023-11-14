package tech.mlsql.retrieval.batchserver;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import tech.mlsql.retrieval.RetrievalWorker;

import java.io.IOException;

/**
 * 11/9/23 WilliamZhu(allwefantasy@gmail.com)
 */
public class BatchServer {

    private final String host;
    private int port;

    private RetrievalWorker retrievalWorker;

    public BatchServer(RetrievalWorker worker,String host, int port) {
        this.host = host;
        this.port = port;
        this.retrievalWorker = worker;
    }

    public void start() {
        Location location = Location.forGrpcInsecure(host, port);
        try (BufferAllocator allocator = new RootAllocator()) {
            // Server
            try (final LuceneProducer producer = new LuceneProducer(retrievalWorker,allocator, location);
                 final FlightServer flightServer = FlightServer.builder(allocator, location, producer).build()) {
                try {
                    flightServer.start();
                    System.out.println("Server (Location): Listening on port " + flightServer.getPort());
                    port = flightServer.getPort();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

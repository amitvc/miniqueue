package org.miniqueue.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A simple performance test client for the MiniQueue.
 * * This client will spawn multiple threads to hammer the /produce endpoint,
 * simulating concurrent users. It then collects all latency data and
 * prints a report on throughput and latency percentiles.
 */
public class PerfTest {

    // --- Configuration ---
    private static final String SERVER_URL = "http://localhost:3000";

    // Total number of concurrent users to simulate
    private static final int NUM_THREADS = 10;

    // Total number of messages *each* thread will send
    private static final int MESSAGES_PER_THREAD = 10000;

    // A small payload to make the test realistic
    private static final String PADDING = "a".repeat(60); // 100-byte padding
    // ---------------------

    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Histogram latencies = metrics.histogram("produce-latencies");
    private static final Counter successCounter = metrics.counter("produce-success");
    private static final Counter failureCounter = metrics.counter("produce-failure");

    public static void main(String[] args) throws Exception {
        System.out.println("--- MiniQueue Performance Test ---");
        System.out.printf("Configuration: %d Threads, %d Messages/Thread\n", NUM_THREADS, MESSAGES_PER_THREAD);
        System.out.printf("Total Messages: %d\n\n", NUM_THREADS * MESSAGES_PER_THREAD);

        // Create a single, thread-safe HttpClient
        HttpClient client = HttpClient.newBuilder()
                                      .version(HttpClient.Version.HTTP_1_1)
                                      .connectTimeout(Duration.ofSeconds(10))
                                      .build();

        // Create a thread pool to run our "users"
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        // Create a list of tasks (one per user/thread)
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            // Distribute load across partitions 0-100
            final short partitionId = (short)ThreadLocalRandom.current().nextInt(0, 101);
            tasks.add(new ProducerTask(client, partitionId, MESSAGES_PER_THREAD));
        }

        System.out.println("Starting all producer threads...");
        long startTime = System.nanoTime();

        // Run all tasks and wait for them to complete
        executor.invokeAll(tasks);

        long endTime = System.nanoTime();
        System.out.println("...All threads complete.");

        // --- Report Generation ---
        double totalTimeSeconds = (endTime - startTime) / 1_000_000_000.0;
        long totalMessages = (long) NUM_THREADS * MESSAGES_PER_THREAD;

        Snapshot snapshot = latencies.getSnapshot();

        // --- Print Report ---
        System.out.println("\n--- Performance Report ---");
        System.out.printf("Total Time:     %.2f seconds\n", totalTimeSeconds);
        System.out.printf("Total Messages: %d\n", totalMessages);
        System.out.printf("Throughput:     %.2f Msgs/sec\n", totalMessages / totalTimeSeconds);
        System.out.printf("Req Success:     %d Msgs/sec\n", successCounter.getCount());
        System.out.printf("Req Failed:      %d Msgs/sec\n", failureCounter.getCount());

        System.out.println("\n--- Latency Report (ms) ---");
        System.out.printf("p50 (Median): %.2f ms\n", snapshot.getMedian() / 1_000_000.0);
        System.out.printf("p95:          %.2f ms\n", snapshot.get95thPercentile() / 1_000_000.0);
        System.out.printf("p99:          %.2f ms\n", snapshot.get99thPercentile() / 1_000_000.0);
        System.out.printf("p99.9:        %.2f ms\n", snapshot.get999thPercentile() / 1_000_000.0);
        System.out.printf("Max:          %.2f ms\n", snapshot.getMax() / 1_000_000.0);

        executor.shutdown();
    }


    private static class ProducerTask implements Callable<Void> {
        private final HttpClient client;
        private final short partitionId;
        private final int numMessages;

        public ProducerTask(HttpClient client, short partitionId, int numMessages) {
            this.client = client;
            this.partitionId = partitionId;
            this.numMessages = numMessages;
        }

        @Override
        public Void call() throws Exception {
            URI uri = URI.create(SERVER_URL + "/produce/" + partitionId);

            for (int i = 0; i < numMessages; i++) {
                String key = "key-" + partitionId + "-" + i;
                String value = "val-" + PADDING; // Use padding for realistic size
                String body = key + ":" + value;

                HttpRequest request = HttpRequest.newBuilder()
                                                 .uri(uri)
                                                 .POST(HttpRequest.BodyPublishers.ofString(body))
                                                 .build();

                long reqStartTime = System.nanoTime();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                long reqEndTime = System.nanoTime();

                if (response.statusCode() != 200) {
                    System.err.printf("Error: Thread %d got status %d: %s\n",
                                      Thread.currentThread().getId(), response.statusCode(), response.body());
                    failureCounter.inc();
                } else {
                    latencies.update(reqEndTime - reqStartTime);
                    successCounter.inc();
                }
            }
            return null;
        }
    }


}
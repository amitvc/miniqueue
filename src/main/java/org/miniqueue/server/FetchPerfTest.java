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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FetchPerfTest {

    private static final String SERVER_URL = "http://localhost:3000";
    private static final int NUM_THREADS = 4;
    private static final int FETCHES_PER_THREAD = 5_000;

    private static final MetricRegistry METRICS = new MetricRegistry();
    private static final Histogram LATENCIES = METRICS.histogram("fetch-latencies");
    private static final Counter SUCCESS = METRICS.counter("fetch-success");
    private static final Counter FAILURE = METRICS.counter("fetch-failure");

    public static void main(String[] args) throws Exception {
        System.out.println("--- MiniQueue Fetch Performance Test ---");
        System.out.printf("Threads=%d  Fetches/Thread=%d%n", NUM_THREADS, FETCHES_PER_THREAD);

        HttpClient client = HttpClient.newBuilder()
                                      .version(HttpClient.Version.HTTP_1_1)
                                      .connectTimeout(Duration.ofSeconds(10))
                                      .build();

        short[] activePartitions = fetchActivePartitions(client);
        if (activePartitions.length == 0) {
            throw new IllegalStateException("Admin API returned zero partitions with data; seed the queue first.");
        }
        System.out.printf("Targeting %d populated partitions.%n", activePartitions.length);

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            tasks.add(new FetchTask(client, activePartitions, FETCHES_PER_THREAD));
        }

        System.out.println("Starting fetch threads...");
        long start = System.nanoTime();
        executor.invokeAll(tasks);
        long end = System.nanoTime();
        executor.shutdown();
        System.out.println("All fetch threads completed.");

        long totalFetches = (long) NUM_THREADS * FETCHES_PER_THREAD;
        double durationSeconds = (end - start) / 1_000_000_000.0;
        Snapshot snapshot = LATENCIES.getSnapshot();

        System.out.println("\n--- Fetch Performance Report ---");
        System.out.printf("Total Requests: %d%n", totalFetches);
        System.out.printf("Total Time: %.2f s%n", durationSeconds);
        System.out.printf("Throughput: %.2f req/s%n", totalFetches / durationSeconds);
        System.out.printf("Successes: %d%n", SUCCESS.getCount());
        System.out.printf("Failures: %d%n", FAILURE.getCount());

        System.out.println("\n--- Fetch Latency (ms) ---");
        System.out.printf("p50: %.2f%n", snapshot.getMedian() / 1_000_000.0);
        System.out.printf("p95: %.2f%n", snapshot.get95thPercentile() / 1_000_000.0);
        System.out.printf("p99: %.2f%n", snapshot.get99thPercentile() / 1_000_000.0);
        System.out.printf("p99.9: %.2f%n", snapshot.get999thPercentile() / 1_000_000.0);
        System.out.printf("Max: %.2f%n", snapshot.getMax() / 1_000_000.0);
    }

    private static short[] fetchActivePartitions(HttpClient client) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(URI.create(SERVER_URL + "/admin/partitions"))
                                         .GET()
                                         .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Admin endpoint failed: status=" + response.statusCode()
                                            + " body=" + response.body());
        }

        String body = response.body().trim();
        if (body.length() <= 2 || "{}".equals(body)) {
            return new short[0];
        }

        Pattern pattern = Pattern.compile("\"(\\d+)\"\\s*:");
        Matcher matcher = pattern.matcher(body);
        List<Short> partitions = new ArrayList<>();
        while (matcher.find()) {
            partitions.add(Short.parseShort(matcher.group(1)));
        }
        short[] result = new short[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
            result[i] = partitions.get(i);
        }
        return result;
    }

    private static final class FetchTask implements Callable<Void> {
        private final HttpClient client;
        private final short[] partitions;
        private final int numRequests;

        FetchTask(HttpClient client, short[] partitions, int numRequests) {
            this.client = client;
            this.partitions = partitions;
            this.numRequests = numRequests;
        }

        @Override
        public Void call() throws Exception {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < numRequests; i++) {
                short partition = partitions[random.nextInt(partitions.length)];
                URI uri = URI.create(SERVER_URL + "/fetch/" + partition);

                HttpRequest request = HttpRequest.newBuilder()
                                                 .uri(uri)
                                                 .GET()
                                                 .build();

                long start = System.nanoTime();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                long end = System.nanoTime();

                if (response.statusCode() == 200) {
                    LATENCIES.update(end - start);
                    SUCCESS.inc();
                } else {
                    FAILURE.inc();
                    System.err.printf("Fetch failed: thread=%d partition=%d status=%d%n",
                                      Thread.currentThread().getId(), partition, response.statusCode());
                }
            }
            return null;
        }
    }
}

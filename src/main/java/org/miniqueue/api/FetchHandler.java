package org.miniqueue.api;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.miniqueue.storage.EventStorage;
import org.miniqueue.storage.Record;

/**
 * Http Handler for Fetch command
 */
public class FetchHandler extends MiniQueueApi {
    private final Map<Short, ReentrantLock> partitionLocks;
    private final EventStorage eventStorage;
    private final Map<Short, List<Record>> inMemoryCache;
    private final Counter fetchRequests;
    private final Timer timer;

    public FetchHandler(EventStorage eventStorage,
                        Map<Short, ReentrantLock> partitionLocks,
                        Map<Short, List<Record>> inMemoryCache, MetricRegistry metrics) {
        this.partitionLocks = partitionLocks;
        this.eventStorage = eventStorage;
        this.inMemoryCache = inMemoryCache;
        this.fetchRequests = metrics.counter("fetch-requests");
        timer = metrics.timer("fetch-handler");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        fetchRequests.inc();
        Context ctx = timer.time();
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }        short partitionId;
        try {
            String path = exchange.getRequestURI().getPath();
            String idStr = path.substring(path.lastIndexOf('/') + 1);
            partitionId = Short.parseShort(idStr);
            if (partitionId < 0 || partitionId > 100) {
                throw new NumberFormatException("Partition ID must be 0-100.");
            }
        } catch (Exception e) {
            sendResponse(exchange, 400, "Bad Request: Invalid Partition ID");
            return;
        }
        StringBuilder response = new StringBuilder();
        final Lock lock = partitionLocks.computeIfAbsent(partitionId, k -> new ReentrantLock());
        lock.lock();
        try {
            List<Record> committedDataRecords = new ArrayList<>();
            committedDataRecords.addAll(
                    eventStorage.getRecordsByPartition(partitionId));
            List<Record> hotRecords = inMemoryCache.getOrDefault(partitionId, new ArrayList<>());

            // We dont need to sort since data file records will always be the older records than the in memory cache.
            committedDataRecords.addAll(hotRecords);

            for (Record rec : committedDataRecords) {
                response.append(String.format("%d. key=\"%s\" value=\"%s\"\n",
                                              rec.getOffset(), new String(rec.getKey()),
                                              new String(rec.getValue())));
            }

        } catch (Exception e) {
            sendResponse(exchange, 500, "Internal Server Error: " + e.getMessage());
        } finally {
            lock.unlock();
            ctx.stop();
        }
        sendResponse(exchange, 200, response.toString());
    }
}

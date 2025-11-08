package org.miniqueue.api;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.miniqueue.storage.Record;
import org.miniqueue.storage.WalManager;
import org.miniqueue.storage.WalRecord;

/**
 * Http handler for Produce command
 */
public class ProduceHandler extends MiniQueueApi {

    private final Map<Short, ReentrantLock> partitionLocks;
    private final Map<Short, List<Record>> inMemoryCache;
    private final WalManager walManager;
    private final Map<Short, Long> partitionOffsets;
    private final Counter produceRequests;
    private final Timer timer;

    public ProduceHandler(Map<Short, ReentrantLock> partitionLocks,
                          Map<Short, List<Record>> inMemoryCache,
                          Map<Short, Long> partitionOffsets,
                          WalManager walManager, MetricRegistry metrics) {
        this.partitionLocks = partitionLocks;
        this.inMemoryCache = inMemoryCache;
        this.walManager = walManager;
        this.partitionOffsets = partitionOffsets;
        produceRequests = metrics.counter("produce-requests");
        timer = metrics.timer("produce-handler");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        produceRequests.inc();
        Context ctx = timer.time();
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }
        short partitionId;
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
        String key, value;
        try (InputStream is = exchange.getRequestBody()) {
            byte[] bodyBytes = is.readAllBytes();
            String body = new String(bodyBytes, "UTF-8");
            String[] parts = body.split(":", 2);
            if (parts.length != 2) {
                throw new IOException("Body must be in 'key:value' format.");
            }
            key = parts[0];
            value = parts[1];
        } catch (Exception e) {
            sendResponse(exchange, 400, "Bad Request: " + e.getMessage());
            return;
        }
        final Lock lock = partitionLocks.computeIfAbsent(partitionId, k -> new ReentrantLock());
        lock.lock();
        try {
            /**
             *  Step 1 : Get the partitional offset
             *  Step 2 : write the entry to the WAL
             *  Step 3 : Write the entry to the in-memory cache which will be flushed to disk data pages
             */

            long offset = partitionOffsets.getOrDefault(partitionId, 0L);
            WalRecord entry = new WalRecord(partitionId, offset, key.getBytes(StandardCharsets.UTF_8),
                                            value.getBytes(
                                                    StandardCharsets.UTF_8));
            walManager.durableProduce(entry);
            inMemoryCache.computeIfAbsent(partitionId, k -> new ArrayList<>())
                                         .add(entry);
            partitionOffsets.put(partitionId, offset + 1);
            sendResponse(exchange, 200, String.valueOf(offset));
        } catch (Exception e) {
            sendResponse(exchange, 500, "Internal Server Error: " + e.getMessage());
        } finally {
            lock.unlock();
            ctx.stop();
        }
    }
}

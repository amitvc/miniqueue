package org.miniqueue.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronously flushes WAL entries so producers only wait for enqueue.
 * We can tune the settings for maxBatchSize and flushInterval based on our risk acceptance criteria
 *
 */
public class AsyncWalWriter implements AutoCloseable {

    private final WalManager walManager;
    private final BlockingQueue<WalRecord> queue = new LinkedBlockingQueue<>();
    private final int maxBatchSize;
    private final long flushIntervalMs;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread worker;

    public AsyncWalWriter(WalManager walManager, int maxBatchSize, long flushIntervalMs) {
        this.walManager = walManager;
        this.maxBatchSize = Math.max(1, maxBatchSize);
        this.flushIntervalMs = Math.max(1, flushIntervalMs);
        this.worker = new Thread(this::processQueue, "async-wal-writer");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public void append(WalRecord record) {
        if (record == null) {
            return;
        }
        queue.add(record);
    }

    private void processQueue() {
        List<WalRecord> batch = new ArrayList<>(maxBatchSize);
        try {
            while (running.get() || !queue.isEmpty()) {
                WalRecord first;
                try {
                    first = queue.poll(flushIntervalMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (!running.get()) {
                        first = null;
                    } else {
                        continue;
                    }
                }
                if (first != null) {
                    batch.add(first);
                    queue.drainTo(batch, maxBatchSize - batch.size());
                }
                if (!batch.isEmpty() && (batch.size() >= maxBatchSize || first == null)) {
                    walManager.appendBatch(batch, true);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                walManager.appendBatch(batch, true);
            }
        } catch (IOException e) {
            System.err.println("Error in Async writer " + e.getMessage());
            // TODO handle this and emit metrics.
        }
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        worker.interrupt();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        walManager.flushCurrentSegment();
    }
}

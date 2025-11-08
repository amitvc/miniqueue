package org.miniqueue.server;

import static org.miniqueue.util.Config.CHECKPOINT_FILE_PATH;
import static org.miniqueue.util.Config.COMPACTOR_INTERVAL_MS;
import static org.miniqueue.util.Config.DATA_FILE_NAME;
import static org.miniqueue.util.Config.DEFAULT_PAGE_SIZE;
import static org.miniqueue.util.Config.FLUSH_INTERVAL_MS;
import static org.miniqueue.util.Config.MAX_PARTITION_ID;
import static org.miniqueue.util.Config.MAX_WAL_SEGMENT_SIZE;
import static org.miniqueue.util.Config.WAL_FILE_NAME;
import static org.miniqueue.util.Config.WAL_FLUSH_INTERVAL_MS;
import static org.miniqueue.util.Config.WAL_MAX_BATCH_SIZE;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.miniqueue.api.AdminHandler;
import org.miniqueue.api.FetchHandler;
import org.miniqueue.api.ProduceHandler;
import org.miniqueue.storage.AsyncWalWriter;
import org.miniqueue.storage.CheckpointManager;
import org.miniqueue.storage.Event;
import org.miniqueue.storage.LocalDiskEventStorage;
import org.miniqueue.storage.Record;
import org.miniqueue.storage.WalManager;
import org.miniqueue.storage.WalRecord;

public class MiniQueueServer implements AutoCloseable {

    private final HttpServer server;
    private final WalManager walManager;
    private final LocalDiskEventStorage localDiskEventStorage;
    private final AsyncWalWriter asyncWalWriter;
    private final ScheduledExecutorService flusherThread;
    private final ScheduledExecutorService compactorThread;
    private final CheckpointManager checkpointManager;
    private final MetricRegistry metrics = new MetricRegistry();
    private final ConsoleReporter reporter;
    private final Counter flushCounter;
    private final Counter compactionCounter;


    // In-memory state
    private final Map<Short, ReadWriteLock> partitionLocks = new ConcurrentHashMap<>();
    private final Map<Short, List<Record>> inMemoryCache = new ConcurrentHashMap<>();

    // The current offset of each partition. This does not mean the offset is committed to the data pages yet.
    private final Map<Short, Long> partitionOffsets = new ConcurrentHashMap<>();

    // The current offset of each partition. The offset is of the records that are flushed to the data pages.
    private final Map<Short, Long> flushedOffsets;


    public void start() {
        server.start();
    }

    public MiniQueueServer(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        walManager = new WalManager(WAL_FILE_NAME, MAX_WAL_SEGMENT_SIZE);
        checkpointManager = new CheckpointManager(CHECKPOINT_FILE_PATH);
        localDiskEventStorage = new LocalDiskEventStorage(DATA_FILE_NAME, DEFAULT_PAGE_SIZE);
        // On startup load all checkpoint offsets for each partition.
        // This information is stored in miniqueue.checkpoint file. This file gets updated at everytime we
        // flush the inmemory cache to the checkpoint file. See flushCacheToDataFile() for details.
        flushedOffsets = checkpointManager.loadCheckpoint();
        recoverFromWal(flushedOffsets);
        asyncWalWriter = new AsyncWalWriter(walManager, WAL_MAX_BATCH_SIZE, WAL_FLUSH_INTERVAL_MS);


        // Setup the http endpoints.
        // In a production service I would use something like netty or spring/boot to setup API endpoints.
        // Keeping dependencies to minimum.
        server.createContext("/produce/", new ProduceHandler(partitionLocks, inMemoryCache, partitionOffsets, asyncWalWriter, metrics));
        server.createContext("/fetch/", new FetchHandler(localDiskEventStorage, partitionLocks, inMemoryCache, metrics));
        server.createContext("/admin/", new AdminHandler(localDiskEventStorage));

        // This thread flushes the in-memory cache of records to the data pages.
        // Remember the record is already been written and flushed to WAL. So even if we crash we can recover.
        flusherThread = Executors.newSingleThreadScheduledExecutor();
        flusherThread.scheduleAtFixedRate(
                this::flushCacheToDataFile,
                FLUSH_INTERVAL_MS,
                FLUSH_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        // This thread runs in the background and truncates any wal logs whose
        // entries have been flushed to data files
        compactorThread = Executors.newSingleThreadScheduledExecutor();
        compactorThread.scheduleAtFixedRate(
                this::truncateWalSegments,
                COMPACTOR_INTERVAL_MS, COMPACTOR_INTERVAL_MS, TimeUnit.MILLISECONDS
        );

        flushCounter = metrics.counter("flush-counter");
        compactionCounter = metrics.counter("compaction-counter");

        reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);


    }

    /**
     *  When the server starts it will try to recover from WAL if some events have not yet been flushed to data
     *  file.
     * @param flushedOffsets Map<PartitionId, Offset>
     * @throws IOException
     */
    private void recoverFromWal(final Map<Short, Long> flushedOffsets) throws IOException {
        System.out.println("Starting WAL recovery...");
        List<WalRecord> walRecords = walManager.readAllWalRecords();
        long recoveredCount = 0;

        // 1. Filter out records that have already been flushed
        List<WalRecord> unflushedRecords = walRecords.stream()
                .filter(record -> {
                    long flushedOffset = flushedOffsets.getOrDefault(record.getPartitionId(), -1L);
                    return record.getOffset() > flushedOffset;
                })
                .collect(Collectors.toList());

        if (unflushedRecords.isEmpty()) {
            System.out.println("No unflushed records found in WAL. Recovery not needed.");
            return;
        }

        // 2. Group the unflushed records by partition
        Map<Short, List<WalRecord>> groupedByPartition = unflushedRecords.stream()
                .collect(Collectors.groupingBy(WalRecord::getPartitionId));

        // 3. Add the recovered records to the in-memory cache
        for (Map.Entry<Short, List<WalRecord>> entry : groupedByPartition.entrySet()) {
            short partitionId = entry.getKey();
            List<WalRecord> recordsToRecover = entry.getValue();

            // Make sure they are sorted by offset before adding back
            recordsToRecover.sort(java.util.Comparator.comparingLong(WalRecord::getOffset));

            // Get the lock for the partition
            ReadWriteLock partitionLock = getPartitionLock(partitionId);
            Lock writeLock = partitionLock.writeLock();
            writeLock.lock();
            try {
                // Add to in-memory cache
                List<Record> partitionCache = inMemoryCache.computeIfAbsent(partitionId, k -> new ArrayList<>());
                for (WalRecord walRecord : recordsToRecover) {
                    partitionCache.add(new Event(walRecord.getOffset(), walRecord.getKey(), walRecord.getValue()));
                    recoveredCount++;
                }

                // Update the partition offset to the latest recovered offset
                if (!recordsToRecover.isEmpty()) {
                    long lastOffset = recordsToRecover.get(recordsToRecover.size() - 1).getOffset();
                    partitionOffsets.put(partitionId, lastOffset);
                }
            } finally {
                writeLock.unlock();
            }
        }

        System.out.println("WAL Recovery complete. Recovered " + recoveredCount + " records into memory.");
    }


    private void flushCacheToDataFile() {
        Map<Short, List<Record>> cacheToFlush = new HashMap<>();
        Map<Short, Long> newFlushedOffsets = new HashMap<>();

        for (short partitionId = 0; partitionId <= MAX_PARTITION_ID; partitionId++) {
            ReadWriteLock partitionLock = getPartitionLock(partitionId);
            Lock writeLock = partitionLock.writeLock();
            writeLock.lock();
            try {
                List<Record> entries = inMemoryCache.get(partitionId);
                if (entries != null && !entries.isEmpty()) {
                    cacheToFlush.put(partitionId, entries);

                    // Flush the in-memory cache for the partition.
                    inMemoryCache.put(partitionId, new ArrayList<>());

                    // Find the highest offset in this batch
                    long maxOffset = entries.get(entries.size() - 1).getOffset();
                    newFlushedOffsets.put(partitionId, maxOffset);
                }
            } finally {
                writeLock.unlock();
            }
        }

        if (cacheToFlush.isEmpty()) {
            return; // nothing to do.
        }

        try {
            // Technically these are in-memory entries which have been written to wal for durability.
            localDiskEventStorage.flushWalEntries(cacheToFlush, flushCounter);

            // Atomically save the new "safe" offsets
            // We must merge with the old map in case we didn't flush all partitions
            Map<Short, Long> finalCheckpoint = checkpointManager.loadCheckpoint();
            finalCheckpoint.putAll(newFlushedOffsets);

            checkpointManager.saveCheckpoint(finalCheckpoint);

            // Also update our in-memory copy
            flushedOffsets.putAll(finalCheckpoint);


        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("CRITICAL: FlusherThread failed! " + e.getMessage());
            // TODO: Put cacheToFlush items back into inMemoryCache for retry
        }
    }

    private void truncateWalSegments() {
        System.out.println("CompactorThread: Waking up to check for old WAL segments...");
        try {
            // Get a snapshot of the *current* safe offsets
            Map<Short, Long> currentFlushedOffsets = flushedOffsets;

            List<Path> segments = walManager.getWalSegments();

            // We can never delete the *last* (active) segment
            if (segments.size() <= 1) {
                System.out.println("CompactorThread: Not enough segments to compact.");
                return;
            }

            // Check all segments *except* the last one
            for (int i = 0; i < segments.size() - 1; i++) {
                Path segmentPath = segments.get(i);
                if (walManager.isSegmentSafeToTruncate(segmentPath, currentFlushedOffsets)) {
                    // This segment is 100% safe to delete.
                    compactionCounter.inc();
                    walManager.truncateSegment(segmentPath);
                } else {
                    // We must stop here. If wal-001 is not safe,
                    // we can't check wal-002, as order is not guaranteed.
                    // A safer design would check them all, but this is fine.
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("CompactorThread: Failed to truncate WAL: " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        System.out.println("Shutting down server...");
        server.stop(1);
        flusherThread.shutdown();
        compactorThread.shutdown(); // Shut down new thread
        reporter.stop();
        try {
            flusherThread.awaitTermination(5, TimeUnit.SECONDS);
            compactorThread.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Background threads did not shut down cleanly.");
        }
        asyncWalWriter.close();
        walManager.close();
        localDiskEventStorage.close();
        System.out.println("Server shut down.");
    }

    private ReadWriteLock getPartitionLock(short partitionId) {
        return partitionLocks.computeIfAbsent(partitionId, k -> new ReentrantReadWriteLock(true));
    }

}

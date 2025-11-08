package org.miniqueue.storage;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Manages all I/O for the WAL, which is now a series of segment files.
 * Format: wal-segment-0000000000000000001.log
 */
public class WalManager implements AutoCloseable {

    private final Path walDirectory;
    private final long maxSegmentSizeBytes;
    private final ReentrantLock walLock = new ReentrantLock(); // Global lock for switching segments

    private long currentSegmentId;
    private FileChannel currentSegmentChannel;
    private long currentSegmentSize;

    public WalManager(String walDirectoryPath, long maxSegmentSizeBytes) throws IOException {
        this.walDirectory = Paths.get(walDirectoryPath);
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
        Files.createDirectories(walDirectory);

        // Find the latest segment or create a new one
        this.currentSegmentId = getWalSegments().stream()
                                                .map(this::segmentIdFromPath)
                                                .max(Long::compareTo)
                                                .orElse(1L); // Start at 1

        openCurrentSegment();
    }

    private void openCurrentSegment() throws IOException {
        // Close existing segment
        if (currentSegmentChannel != null) {
            currentSegmentChannel.force(true);
            currentSegmentChannel.close();
        }
        Path segmentPath = segmentPathFromId(currentSegmentId);
        currentSegmentChannel = FileChannel.open(segmentPath,
                                                      StandardOpenOption.WRITE,
                                                      StandardOpenOption.CREATE,
                                                      StandardOpenOption.APPEND); // Append mode
        currentSegmentSize = currentSegmentChannel.size();
    }

    public void durableProduce(WalRecord entry) throws IOException {
        walLock.lock();
        try {
            writeRecord(entry);
            currentSegmentChannel.force(false); // false = fdatasync (faster)
        } finally {
            walLock.unlock();
        }
    }

    void appendBatch(List<WalRecord> entries, boolean forceAfterWrite) throws IOException {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        walLock.lock();
        try {
            for (WalRecord entry : entries) {
                writeRecord(entry);
            }
            if (forceAfterWrite) {
                currentSegmentChannel.force(false);
            }
        } finally {
            walLock.unlock();
        }
    }

    void flushCurrentSegment() throws IOException {
        walLock.lock();
        try {
            if (currentSegmentChannel != null) {
                currentSegmentChannel.force(true);
            }
        } finally {
            walLock.unlock();
        }
    }

    /**
     * Reads all records from all WAL segments.
     */
    public List<WalRecord> readAllWalRecords() throws IOException {
        List<WalRecord> records = new ArrayList<>();
        List<Path> segments = getWalSegments();

        for (Path segmentPath : segments) {
            try (FileInputStream fis = new FileInputStream(segmentPath.toFile());
                 DataInputStream dis = new DataInputStream(fis)) {

                while (true) {
                    try {
                        records.add(WalRecord.deserialize(dis));
                    } catch (EOFException e) {
                        break; // End of file
                    }
                }
            }
        }
        return records;
    }

    /**
     * Gets a sorted list of all WAL segment paths, from oldest to newest.
     */
    public List<Path> getWalSegments() throws IOException {
        try (var stream = Files.list(walDirectory)) {
            return stream
                    .filter(p -> p.toString().endsWith(".log"))
                    .sorted(Comparator.comparing(this::segmentIdFromPath))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Checks an inactive segment to see if it's safe to delete.
     * A segment is safe if all its entries are older than the
     * latest flushed checkpoint for their respective partitions.
     */
    public boolean isSegmentSafeToTruncate(Path segmentPath, Map<Short, Long> flushedOffsets) {
        if (segmentIdFromPath(segmentPath) == currentSegmentId) {
            return false;
        }

        // We only delete the entire wal file if no entries have an offset greater that last flushed offset.
        try (FileInputStream fis = new FileInputStream(segmentPath.toFile());
             DataInputStream dis = new DataInputStream(fis)) {

            while (true) {
                WalRecord entry;
                try {
                    entry = WalRecord.deserialize(dis);
                } catch (EOFException e) {
                    break; // End of file
                }

                // Check this entry against the checkpoint
                long lastFlushed = flushedOffsets.getOrDefault(entry.getPartitionId(), -1L);
                if (entry.getOffset() > lastFlushed) {
                    // This segment contains an entry that has *not* been flushed.
                    // It is NOT safe to delete.
                    return false;
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading segment " + segmentPath + " for truncation check: " + e.getMessage());
            return false;
        }

        // If we got here then some entries in the wal are not yet written to data file.
        return true;
    }

    /**
     * Deletes a specific WAL segment file.
     */
    public void truncateSegment(Path segmentPath) throws IOException {
        System.out.println("COMPACTOR: Truncating old WAL segment: " + segmentPath.getFileName());
        Files.delete(segmentPath);
    }

    @Override
    public void close() throws IOException {
        walLock.lock();
        try {
            if (currentSegmentChannel != null) {
                currentSegmentChannel.force(true);
                currentSegmentChannel.close();
            }
        } finally {
            walLock.unlock();
        }
    }

    private Path segmentPathFromId(long id) {
        return walDirectory.resolve(String.format("wal-segment-%020d.log", id));
    }

    private long segmentIdFromPath(Path path) {
        String name = path.getFileName().toString();
        String num = name.substring(12, 32); // "wal-segment-" and ".log"
        return Long.parseLong(num);
    }

    private void writeRecord(WalRecord entry) throws IOException {
        ByteBuffer payload = entry.serialize();
        int payloadSize = payload.remaining();

        ByteBuffer buffer = ByteBuffer.allocate(4 + payloadSize);
        buffer.putInt(payloadSize);
        buffer.put(payload);
        buffer.flip();

        ensureCapacity(buffer.remaining());

        while (buffer.hasRemaining()) {
            currentSegmentChannel.write(buffer);
        }
        currentSegmentSize += (payloadSize + Integer.BYTES);
    }

    private void ensureCapacity(int bytesNeeded) throws IOException {
        if (currentSegmentSize + bytesNeeded > maxSegmentSizeBytes) {
            currentSegmentId++;
            openCurrentSegment();
        }
    }
}

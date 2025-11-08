package org.miniqueue.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the checkpoint file (`miniqueue.checkpoint`).
 * This file stores the highest offset flushed to the .dat file
 * for each partition. This is useful information during disaster recovery
 * We will only replay those records for a partition who have not been checkpointed.
 */
public class CheckpointManager {

    private final Path checkpointPath;
    private final Path tempCheckpointPath;

    public CheckpointManager(String checkpointFilePath) {
        checkpointPath = Paths.get(checkpointFilePath);
        tempCheckpointPath = Paths.get(checkpointFilePath + ".tmp");
    }


    public void saveCheckpoint(Map<Short, Long> flushedOffsets) throws IOException {
        // Write to a temporary file first
        try (FileOutputStream fos = new FileOutputStream(tempCheckpointPath.toFile());
             DataOutputStream dos = new DataOutputStream(fos)) {

            dos.writeInt(flushedOffsets.size()); // Write header (count)
            for (Map.Entry<Short, Long> entry : flushedOffsets.entrySet()) {
                dos.writeShort(entry.getKey());
                dos.writeLong(entry.getValue());
            }
            // fsync the temp file
            fos.getFD().sync();
        }
        // Just replace the file.
        Files.move(tempCheckpointPath, checkpointPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }


    public Map<Short, Long> loadCheckpoint() {
        Map<Short, Long> flushedOffsets = new ConcurrentHashMap<>();
        if (!Files.exists(checkpointPath)) {
            return flushedOffsets; // No checkpoint yet, return empty map
        }

        try (FileInputStream fis = new FileInputStream(checkpointPath.toFile());
             DataInputStream dis = new DataInputStream(fis)) {

            int size = dis.readInt();
            for (int i = 0; i < size; i++) {
                short partitionId = dis.readShort();
                long offset = dis.readLong();
                flushedOffsets.put(partitionId, offset);
            }
        } catch (IOException e) {
            System.err.println("Could not read checkpoint file, starting with empty state. Error: " + e.getMessage());
            return new ConcurrentHashMap<>(); // Return empty on error
        }
        return flushedOffsets;
    }
}
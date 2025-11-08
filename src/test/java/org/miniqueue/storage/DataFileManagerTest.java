package org.miniqueue.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataFileManagerTest {

    @TempDir
    Path tempDir;

    private LocalDiskEventStorage localDiskEventStorage;
    private String dataFilePath;
    private final int pageSize = 1024;

    @BeforeEach
    void setUp() throws IOException {
        File dataFile = tempDir.resolve("data.bin").toFile();
        dataFilePath = dataFile.getAbsolutePath();
        localDiskEventStorage = new LocalDiskEventStorage(dataFilePath, pageSize);
    }

    @AfterEach
    void tearDown() throws IOException {
        localDiskEventStorage.close();
    }

    @Test
    void testNewDataFileManager() throws IOException {
        File dataFile = new File(dataFilePath);
        assertTrue(dataFile.exists());
        // Header page + metadata pages
        long expectedSize = (long) pageSize * (1 + 101);
        assertEquals(expectedSize, dataFile.length());
    }

    @Test
    void testFlushAndGetRecords() throws IOException {
        // 1. Flush some records
        short partitionId = 1;
        List<Record> records = new ArrayList<>();
        records.add(new WalRecord(partitionId, 1L, "key1".getBytes(), "value1".getBytes()));
        records.add(new WalRecord(partitionId, 2L, "key2".getBytes(), "value2".getBytes()));
        Map<Short, List<Record>> groupedRecords = new HashMap<>();
        groupedRecords.put(partitionId, records);

        localDiskEventStorage.flushWalEntries(groupedRecords, new com.codahale.metrics.Counter());

        // 2. Get the records back
        List<Record> fetchedRecords = localDiskEventStorage.getRecordsByPartition(partitionId);
        assertEquals(2, fetchedRecords.size());
        assertEquals(1L, fetchedRecords.get(0).getOffset());
        assertArrayEquals("key1".getBytes(), fetchedRecords.get(0).getKey());
        assertArrayEquals("value1".getBytes(), fetchedRecords.get(0).getValue());
        assertEquals(2L, fetchedRecords.get(1).getOffset());
        assertArrayEquals("key2".getBytes(), fetchedRecords.get(1).getKey());
        assertArrayEquals("value2".getBytes(), fetchedRecords.get(1).getValue());
    }

    @Test
    void testCloseAndReopen() throws IOException {
        // 1. Flush some records
        short partitionId = 1;
        List<Record> records = new ArrayList<>();
        records.add(new WalRecord(partitionId, 1L, "key1".getBytes(), "value1".getBytes()));
        Map<Short, List<Record>> groupedRecords = new HashMap<>();
        groupedRecords.put(partitionId, records);

        localDiskEventStorage.flushWalEntries(groupedRecords, new com.codahale.metrics.Counter());
        localDiskEventStorage.close();

        // 2. Reopen and get the records back
        localDiskEventStorage = new LocalDiskEventStorage(dataFilePath, pageSize);
        List<Record> fetchedRecords = localDiskEventStorage.getRecordsByPartition(partitionId);
        assertEquals(1, fetchedRecords.size());
        assertEquals(1L, fetchedRecords.get(0).getOffset());
        assertArrayEquals("key1".getBytes(), fetchedRecords.get(0).getKey());
        assertArrayEquals("value1".getBytes(), fetchedRecords.get(0).getValue());
    }
}
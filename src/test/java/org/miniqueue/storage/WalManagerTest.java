package org.miniqueue.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WalManagerTest {

    @TempDir
    Path tempDir;

    private WalManager walManager;

    @BeforeEach
    void setUp() throws IOException {
        walManager = new WalManager(tempDir.toString(), 1024);
    }

    @AfterEach
    void tearDown() throws IOException {
        walManager.close();
    }

    @Test
    void testDurableProduceAndRollover() throws IOException {
        // Produce some records
        for (int i = 0; i < 10; i++) {
            walManager.durableProduce(new WalRecord((short) 1, i, ("key" + i).getBytes(), ("value" + i).getBytes()));
        }

        // The segment size is 1024, so we should have only one segment
        assertEquals(1, walManager.getWalSegments().size());

        // Produce more records to trigger a rollover
        for (int i = 10; i < 100; i++) {
            walManager.durableProduce(new WalRecord((short) 1, i, ("key" + i).getBytes(), ("value" + i).getBytes()));
        }

        assertTrue(walManager.getWalSegments().size() > 1);
    }

    @Test
    void testIsSegmentSafeToTruncate() throws IOException {
        // Produce some records to fill up the first segment and cause a rollover
        for (int i = 0; i < 100; i++) {
            walManager.durableProduce(new WalRecord((short) 1, i, ("key" + i).getBytes(), ("value" + i).getBytes()));
        }

        List<Path> segments = walManager.getWalSegments();
        assertTrue(segments.size() > 1, "Segment rollover did not occur");

        Map<Short, Long> flushedOffsets = new HashMap<>();
        flushedOffsets.put((short) 1, 5L);

        // The first segment should not be safe to truncate because not all offsets are flushed
        assertFalse(walManager.isSegmentSafeToTruncate(segments.get(0), flushedOffsets));

        flushedOffsets.put((short) 1, 99L);
        // The first segment should be safe to truncate because all offsets are flushed
        assertTrue(walManager.isSegmentSafeToTruncate(segments.get(0), flushedOffsets));
    }

    @Test
    void testTruncateSegment() throws IOException {
        // Produce some records
        for (int i = 0; i < 100; i++) {
            walManager.durableProduce(new WalRecord((short) 1, i, ("key" + i).getBytes(), ("value" + i).getBytes()));
        }

        List<Path> segments = walManager.getWalSegments();
        assertTrue(segments.size() > 1);

        Path segmentToTruncate = segments.get(0);
        walManager.truncateSegment(segmentToTruncate);

        assertFalse(Files.exists(segmentToTruncate));
    }
}
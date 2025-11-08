package org.miniqueue.storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataPageTest {

    @Test
    void testNewDataPage() {
        int pageSize = 1024;
        long pageLSN = 12345L;

        DataPage dataPage = new DataPage(pageSize, pageLSN);

        assertEquals(pageSize, dataPage.getBuffer().capacity());
        assertEquals(pageLSN, dataPage.getBuffer().getLong(0));
        assertEquals(16, dataPage.getBuffer().getInt(8));
        assertEquals(0, dataPage.getBuffer().getInt(12));
    }

    @Test
    void testLoadDataPage() {
        int pageSize = 1024;
        long pageLSN = 54321L;
        int currentDataEnd = 100;
        int recordCount = 5;

        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        buffer.putLong(0, pageLSN);
        buffer.putInt(8, currentDataEnd);
        buffer.putInt(12, recordCount);

        DataPage dataPage = new DataPage(buffer);

        assertEquals(pageSize, dataPage.getBuffer().capacity());
        assertEquals(pageLSN, dataPage.getBuffer().getLong(0));
    }

    @Test
    void testAddRecord() {
        int pageSize = 1024;
        long pageLSN = 1L;

        DataPage dataPage = new DataPage(pageSize, pageLSN);

        Event record = new Event(1L, new byte[]{ 1, 2, 3}, new byte[]{ 4, 5, 6});
        assertTrue(dataPage.addRecord(record));

        assertEquals(1, dataPage.getBuffer().getInt(12)); // Record count should be 1
        assertEquals(16 + record.getSizeInBytes(), dataPage.getBuffer().getInt(8)); // Data end should be updated
    }

    @Test
    void testAddRecordPageFull() {
        int pageSize = 32; // Small page size for testing
        long pageLSN = 1L;

        DataPage dataPage = new DataPage(pageSize, pageLSN);

        Event record = new Event(1L, new byte[10], new byte[10]);
        assertFalse(dataPage.addRecord(record));
    }

    @Test
    void testGetRecords() {
        int pageSize = 1024;
        long pageLSN = 1L;

        DataPage dataPage = new DataPage(pageSize, pageLSN);

        Event record1 = new Event(1L, "key1".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8));
        Event record2 = new Event(2L, "key2".getBytes(StandardCharsets.UTF_8), "value2".getBytes(StandardCharsets.UTF_8));

        dataPage.addRecord(record1);
        dataPage.addRecord(record2);

        List<Event> records = dataPage.getRecords();

        assertEquals(2, records.size());
        assertArrayEquals(record1.getKey(), records.get(0).getKey());
        assertArrayEquals(record1.getValue(), records.get(0).getValue());
        assertArrayEquals(record2.getKey(), records.get(1).getKey());
        assertArrayEquals(record2.getValue(), records.get(1).getValue());
    }

    @Test
    void testSetPageLSN() {
        int pageSize = 1024;
        long pageLSN = 1L;

        DataPage dataPage = new DataPage(pageSize, pageLSN);
        dataPage.setPageLSN(2L);
        assertEquals(2L, dataPage.getBuffer().getLong(0));
    }
}
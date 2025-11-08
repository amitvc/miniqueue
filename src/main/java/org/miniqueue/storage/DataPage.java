package org.miniqueue.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the data page. It is how we store
 * entries for messages that are posted to the queue on disk.
 * Layout of this page below
 * +--------------------+--------------------+--------------------+-------------------------------+
 * | Page LSN           | Current Data End   | Record Count       | Raw Page Data (buffer)        |
 * | (long)             | (int)              | (int)              | (byte[pageSize - 16])         |
 * | 8 bytes            | 4 bytes            | 4 bytes            | (variable, depends on pageSize)|
 * +--------------------+--------------------+--------------------+-------------------------------+
 *
 */
public class DataPage {

    private long pageLSN;
    private int currentDataEnd;
    private int recordCount;

    private final ByteBuffer buffer; // The raw page data
    private final int pageSize;

    private static final int PAGE_HEADER_SIZE = 16;


    /**
     * Creates a new, empty DataPage, formatting its buffer.
     */
    public DataPage(int pageSize, long pageLSN) {
        this.pageSize = pageSize;
        buffer = ByteBuffer.allocate(pageSize);
        this.pageLSN = pageLSN;
        recordCount = 0;
        currentDataEnd = PAGE_HEADER_SIZE; // Free space starts after the header
        formatHeader();
    }

    public DataPage(ByteBuffer buffer) {
        this.pageSize = buffer.capacity();
        this.buffer = buffer;

        // Read the header from the buffer's start
        this.pageLSN = buffer.getLong();
        this.currentDataEnd = buffer.getInt();
        this.recordCount = buffer.getInt();
    }

    private void formatHeader() {
        buffer.position(0);
        buffer.putLong(pageLSN);
        buffer.putInt(currentDataEnd);
        buffer.putInt(recordCount);
    }

    public void setPageLSN(long pageLSN) {
        this.pageLSN = pageLSN;
        buffer.position(0);
        buffer.putLong(pageLSN);
    }

    // Fast exits if the page is full.
    public boolean addRecord(Event record) {
        int recordSize = record.getSizeInBytes();

        if (currentDataEnd + recordSize > pageSize) {
            return false; // Page is full. Find another page to write the new record.
        }

        buffer.position(currentDataEnd);
        record.serialize(buffer);

        recordCount++;
        currentDataEnd += recordSize;
        formatHeader();

        return true;
    }

    public List<Event> getRecords() {
        List<Event> records = new ArrayList<>(recordCount);

        buffer.position(PAGE_HEADER_SIZE);
        buffer.limit(currentDataEnd);

        ByteBuffer dataBuffer = buffer.slice();

        while (dataBuffer.hasRemaining()) {
            try {
                Event record = Event.deserialize(dataBuffer);
                records.add(record);
            } catch (Exception e) {
                // This could happen if the page is corrupt
                System.err.println("Failed to deserialize a page record: " + e.getMessage());
                // Stop reading this page
                break;
            }
        }

        if (records.size() != recordCount) {
            System.err.printf(
                    "Warning: Page record count mismatch. Header said %d, but read %d.%n",
                    recordCount, records.size()
            );
        }

        return records;
    }

    public ByteBuffer getBuffer() {
        buffer.position(0);
        buffer.limit(pageSize);
        return buffer;
    }
}
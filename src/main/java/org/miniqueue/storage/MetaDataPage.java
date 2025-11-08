package org.miniqueue.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Record that holds pages owned by a particular partition.
 * It also contains a pointer to the next page if the partition's
 * page list exceeds the page size.
 * File structure:
 * +-------------------------+------------------------+-------------------------+
 * | Next MetaData Page Num  | Page Count             | Page Numbers (list)     |
 * | (int)                   | (int)                  | (int[n]...)             |
 * | 4 bytes                 | 4 bytes                | 4 bytes * pageCount     |
 * +-------------------------+------------------------+-------------------------+
 *
 */
public class MetaDataPage {

    public static final int HEADER_SIZE = Integer.BYTES * 2;

    // Header fields
    private int nextMetaDataPageNumber;
    private int pageCount;

    // Page data.
    private final List<Integer> pageNumbers;

    public MetaDataPage(int nextMetaDataPageNumber, List<Integer> pageNumbers) {
        Objects.requireNonNull(pageNumbers, "pageNumbers cannot be null");
        this.nextMetaDataPageNumber = nextMetaDataPageNumber;
        this.pageNumbers = pageNumbers;
        this.pageCount = this.pageNumbers.size();
    }

    public MetaDataPage() {
        this(0, new ArrayList<>());
    }

    public int getNextMetaDataPageNumber() {
        return nextMetaDataPageNumber;
    }

    public void setNextMetaDataPageNumber(int nextMetaDataPageNumber) {
        this.nextMetaDataPageNumber = nextMetaDataPageNumber;
    }

    public int getPageCount() {
        return pageCount;
    }

    public List<Integer> getPageNumbers() {
        return pageNumbers;
    }

    public int getLastPageNumber() {
        if (pageNumbers.isEmpty()) {
            return 0;
        }
        return pageNumbers.get(pageNumbers.size() - 1);
    }


    public boolean addPageNumber(int pageNumber, int pageSize) {
        if ((HEADER_SIZE + (pageCount + 1) * Integer.BYTES) > pageSize) {
            return false; //fast exit if page is full.
        }
        pageNumbers.add(pageNumber);
        pageCount = pageNumbers.size();
        return true;
    }

    /**
     * Serializes this metadata page into the provided buffer starting at the buffer's
     * current position. The buffer's position is advanced by the number of bytes written.
     */
    public void serialize(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer cannot be null");

        int requiredBytes = HEADER_SIZE + pageCount * Integer.BYTES;
        if (buffer.remaining() < requiredBytes) {
            throw new IllegalArgumentException("Buffer does not have enough remaining bytes to serialize metadata page.");
        }

        ByteBuffer slice = buffer.slice();
        slice.putInt(nextMetaDataPageNumber);
        slice.putInt(pageCount);
        for (int pageNumber : pageNumbers) {
            slice.putInt(pageNumber);
        }

        buffer.position(buffer.position() + requiredBytes);
    }

    public ByteBuffer serialize(int pageSize) {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        serialize(buffer);
        buffer.position(0);
        return buffer;
    }

    /**
     * Deserializes a metadata page from the buffer's current position and advances
     * the buffer by the number of bytes consumed.
     */
    public static MetaDataPage deserialize(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer cannot be null");

        if (buffer.remaining() < HEADER_SIZE) {
            throw new IllegalArgumentException("Buffer does not contain enough data for metadata header.");
        }

        ByteBuffer slice = buffer.slice();
        int nextPageNumber = slice.getInt();
        int pageCount = slice.getInt();

        if (pageCount < 0) {
            throw new IllegalArgumentException("Page count cannot be negative: " + pageCount);
        }

        if (slice.remaining() < (long) pageCount * Integer.BYTES) {
            throw new IllegalArgumentException("Buffer does not contain enough data for the listed page numbers.");
        }

        List<Integer> pageNumbers = new ArrayList<>(pageCount);
        for (int i = 0; i < pageCount; i++) {
            pageNumbers.add(slice.getInt());
        }

        int consumed = HEADER_SIZE + pageCount * Integer.BYTES;
        buffer.position(buffer.position() + consumed);

        return new MetaDataPage(nextPageNumber, pageNumbers);
    }
}
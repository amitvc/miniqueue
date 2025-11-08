package org.miniqueue.storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetaDataPageTest {

    private static final int TEST_PAGE_SIZE = 128;

    @Test
    void testSerializationDeserialization() {
        List<Integer> pageNumbers = new ArrayList<>();
        pageNumbers.add(100);
        pageNumbers.add(200);
        MetaDataPage originalPage = new MetaDataPage(1, pageNumbers);

        ByteBuffer buffer = originalPage.serialize(TEST_PAGE_SIZE);
        buffer.rewind();

        MetaDataPage deserializedPage = MetaDataPage.deserialize(buffer);

        assertEquals(originalPage.getNextMetaDataPageNumber(), deserializedPage.getNextMetaDataPageNumber());
        assertEquals(originalPage.getPageCount(), deserializedPage.getPageCount());
        assertEquals(originalPage.getPageNumbers(), deserializedPage.getPageNumbers());
    }

    @Test
    void testSerializationDeserializationWithBuffer() {
        List<Integer> pageNumbers = new ArrayList<>();
        pageNumbers.add(100);
        pageNumbers.add(200);
        MetaDataPage originalPage = new MetaDataPage(1, pageNumbers);

        ByteBuffer buffer = ByteBuffer.allocate(TEST_PAGE_SIZE);
        originalPage.serialize(buffer);
        buffer.flip();

        MetaDataPage deserializedPage = MetaDataPage.deserialize(buffer);

        assertEquals(originalPage.getNextMetaDataPageNumber(), deserializedPage.getNextMetaDataPageNumber());
        assertEquals(originalPage.getPageCount(), deserializedPage.getPageCount());
        assertEquals(originalPage.getPageNumbers(), deserializedPage.getPageNumbers());
    }

    @Test
    void testAddPageNumber() {
        MetaDataPage metaDataPage = new MetaDataPage();
        assertTrue(metaDataPage.addPageNumber(1, TEST_PAGE_SIZE));
        assertEquals(1, metaDataPage.getPageCount());
        assertEquals(1, metaDataPage.getLastPageNumber());

        assertTrue(metaDataPage.addPageNumber(2, TEST_PAGE_SIZE));
        assertEquals(2, metaDataPage.getPageCount());
        assertEquals(2, metaDataPage.getLastPageNumber());
    }

    @Test
    void testAddPageNumberFull() {
        MetaDataPage metaDataPage = new MetaDataPage();
        assertTrue(metaDataPage.addPageNumber(1, 16));
        assertTrue(metaDataPage.addPageNumber(2, 16));
        assertFalse(metaDataPage.addPageNumber(3, 16));
    }

    @Test
    void testDeserializeTruncated() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(0, 1);
        assertThrows(IllegalArgumentException.class, () -> MetaDataPage.deserialize(buffer));
    }

    @Test
    void testDeserializeCorrupted() {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putInt(0, 1);
        buffer.putInt(4, 2);
        buffer.putInt(8, 100);
        assertThrows(IllegalArgumentException.class, () -> MetaDataPage.deserialize(buffer));
    }
}
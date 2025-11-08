package org.miniqueue.storage;

import org.junit.jupiter.api.Test;
import org.miniqueue.util.Config;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataFileHeaderPageTest {

    @Test
    void testConstructorWithPageSize() {
        int pageSize = 4096;
        DataFileHeaderPage header = new DataFileHeaderPage(pageSize);

        assertEquals(Config.CURRENT_VERSION, header.getVersion());
        assertEquals(pageSize, header.getPageSize());
        assertEquals(Config.METADATA_PAGE_START, header.getMetadataPagesStart());
        assertEquals(Config.METADATA_PAGE_COUNT, header.getMetadataPagesCount());
        assertEquals(Config.DATA_PAGE_START_OFFSET, header.getDataPagesStart());
    }

    @Test
    void testGetMetadataPageForPartition() {
        DataFileHeaderPage header = new DataFileHeaderPage(4096);
        assertEquals(Config.METADATA_PAGE_START, header.getMetadataPageForPartition((short) 0));
        assertEquals(Config.METADATA_PAGE_START + 1, header.getMetadataPageForPartition((short) 1));
        assertEquals(Config.METADATA_PAGE_START + Config.METADATA_PAGE_COUNT - 1, header.getMetadataPageForPartition((short) (Config.METADATA_PAGE_COUNT - 1)));
    }

    @Test
    void testGetMetadataPageForPartitionInvalid() {
        DataFileHeaderPage header = new DataFileHeaderPage(4096);
        assertThrows(IllegalArgumentException.class, () -> header.getMetadataPageForPartition((short) -1));
        assertThrows(IllegalArgumentException.class, () -> header.getMetadataPageForPartition((short) Config.METADATA_PAGE_COUNT));
    }

    @Test
    void testSerializationDeserialization() {
        DataFileHeaderPage originalHeader = new DataFileHeaderPage(4096);
        ByteBuffer buffer = originalHeader.serialize();

        // Create a new header by deserializing the buffer
        DataFileHeaderPage deserializedHeader = new DataFileHeaderPage(buffer);

        assertEquals(originalHeader.getVersion(), deserializedHeader.getVersion());
        assertEquals(originalHeader.getPageSize(), deserializedHeader.getPageSize());
        assertEquals(originalHeader.getMetadataPagesStart(), deserializedHeader.getMetadataPagesStart());
        assertEquals(originalHeader.getMetadataPagesCount(), deserializedHeader.getMetadataPagesCount());
        assertEquals(originalHeader.getDataPagesStart(), deserializedHeader.getDataPagesStart());
    }

    @Test
    void testSerializationDeserializationWithBuffer() {
        DataFileHeaderPage originalHeader = new DataFileHeaderPage(8192);
        ByteBuffer buffer = ByteBuffer.allocate(originalHeader.getSerializedSize());
        originalHeader.serialize(buffer);
        buffer.flip();

        DataFileHeaderPage deserializedHeader = new DataFileHeaderPage(buffer);

        assertEquals(originalHeader.getVersion(), deserializedHeader.getVersion());
        assertEquals(originalHeader.getPageSize(), deserializedHeader.getPageSize());
        assertEquals(originalHeader.getMetadataPagesStart(), deserializedHeader.getMetadataPagesStart());
        assertEquals(originalHeader.getMetadataPagesCount(), deserializedHeader.getMetadataPagesCount());
        assertEquals(originalHeader.getDataPagesStart(), deserializedHeader.getDataPagesStart());
    }

    @Test
    void testCorruptSignature() {
        DataFileHeaderPage originalHeader = new DataFileHeaderPage(4096);
        ByteBuffer buffer = originalHeader.serialize();

        // Corrupt the signature
        buffer.put(2, (byte) 0x00);

        assertThrows(RuntimeException.class, () -> new DataFileHeaderPage(buffer));
    }
}
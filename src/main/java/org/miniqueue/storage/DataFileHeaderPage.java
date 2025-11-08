package org.miniqueue.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.miniqueue.util.Config;

/**
 * The header record for the data file. This is the first thing that gets read when we bootstrap the service
 * Think of it as the Table of contents for the queue service. This data record is very essential for the queue
 * to work correctly when the server crashes or is restarted.
 * Record structure below:
 * +------------------+------------------+--------------------+-----------------------+-----------------------+--------------------+
 * | Signature Length | Signature Data   | Version            | Page Size             | Metadata Pages Start  | Metadata Pages Count |
 * | (short)          | (variable bytes) | (short)            | (int)                 | (int)                 | (int)              |
 * | 2 bytes          | (size=SIGNATURE) | 2 bytes            | 4 bytes               | 4 bytes               | 4 bytes            |
 * +------------------+------------------+--------------------+-----------------------+-----------------------+--------------------+
 * | Data Pages Start |
 * | (int)            |
 * | 4 bytes          |
 * +------------------+
 */
public class DataFileHeaderPage {

    private static final byte[] SIGNATURE = Config.SIGNATURE.getBytes(StandardCharsets.UTF_8);
    private final short version;
    private final int pageSize;
    private final int metadataPagesStart;
    private final int metadataPagesCount;
    private final int dataPagesStart;

    public DataFileHeaderPage(short version, int pageSize, int metadataPagesStart, int metadataPagesCount, int dataPagesStart) {
        this.pageSize = pageSize;
        this.version = version;
        this.metadataPagesStart = metadataPagesStart;
        this.metadataPagesCount = metadataPagesCount;
        this.dataPagesStart = dataPagesStart;
    }

    public DataFileHeaderPage(int pageSize) {
        this(Config.CURRENT_VERSION, pageSize, Config.METADATA_PAGE_START, Config.METADATA_PAGE_COUNT, Config.DATA_PAGE_START_OFFSET);
    }

    public DataFileHeaderPage(ByteBuffer buffer) {
        short len = buffer.getShort();
        byte[] signature = new byte[len];
        buffer.get(signature);
        if (!Arrays.equals(signature, SIGNATURE)) {
            throw new RuntimeException("Data file is corrupt, cannot verify the file signature");
        }
        version = buffer.getShort();
        pageSize = buffer.getInt();
        metadataPagesStart = buffer.getInt();
        metadataPagesCount = buffer.getInt();
        dataPagesStart = buffer.getInt();
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getVersion() {
        return version;
    }

    public int getMetadataPagesStart() {
        return metadataPagesStart;
    }

    public int getMetadataPagesCount() {
        return metadataPagesCount;
    }

    public int getDataPagesStart() {
        return dataPagesStart;
    }

    public int getMetadataPageForPartition(short partitionId) {
        if (partitionId < 0 || partitionId >= metadataPagesCount) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
        }
        return metadataPagesStart + partitionId;
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putShort((short)SIGNATURE.length);
        buffer.put(SIGNATURE);
        buffer.putShort(version);
        buffer.putInt(pageSize);
        buffer.putInt(metadataPagesStart);
        buffer.putInt(metadataPagesCount);
        buffer.putInt(dataPagesStart);
    }

    public int getSerializedSize() {
        return 2 + SIGNATURE.length + 2 + 4 + 4 + 4 + 4;
    }

    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(getSerializedSize());
        serialize(buffer);
        buffer.flip();
        return buffer;
    }
}
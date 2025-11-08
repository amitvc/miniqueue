package org.miniqueue.storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * Represents a single "WAL Entry Record" and provides methods
 * to serialize/deserialize it according to the format below.
 +----------------+----------------+----------------+----------------+----------------+----------------+----------------+
 | Record Length  | Partition ID   | Offset         | Key Length     | Key Data       | Value Length   | Value Data     |
 | (short)        | (short)        | (long)         | (short)        | (bytes...)     | (short)        | (bytes...)     |
 | 2 bytes        | 2 bytes        | 8 bytes        | 2 bytes        | (KeyLength)    | 2 bytes        | (ValueLength)  |
 +----------------+----------------+----------------+----------------+----------------+----------------+----------------+
 *  TODO - We can add CRC for the entry to ensure it is not corrupted.
 */
public class WalRecord implements Record {

    private final short partitionId;
    private final long offset;
    private final byte[] key;
    private final byte[] value;

    public WalRecord(short partitionId, long offset, byte[] key, byte[] value) {
        this.partitionId = partitionId;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    public short getPartitionId() {
        return partitionId;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    public ByteBuffer serialize() {

        // payloadLength = Partition(2) + Offset(8) + KeyLen(2) + Key + ValLen(2) + Val
        final int payloadLength = 2 + 8 + 2 + key.length + 2 + value.length;

        ByteBuffer buffer = ByteBuffer.allocate(payloadLength);

        buffer.putShort(partitionId);

        buffer.putLong(offset);

        buffer.putShort((short) key.length);

        buffer.put(key);

        buffer.putShort((short) value.length);

        buffer.put(value);

        buffer.flip();
        return buffer;
    }

    public static WalRecord readFrom(FileChannel channel) throws IOException {
        // 1. Read the RecordLength
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

        if (channel.read(lengthBuffer) < 4) {
            throw new IOException("Unexpected end of file while reading record length.");
        }
        lengthBuffer.flip(); // reuse the buffer below
        int payloadLength = lengthBuffer.getInt();

        if (payloadLength <= 0) {
            throw new IOException("Invalid record length: " + payloadLength);
        }

        ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLength);
        if (channel.read(payloadBuffer) < payloadLength) {
            throw new IOException("Unexpected end of file while reading payload.");
        }
        payloadBuffer.flip();

        return deserialize(payloadBuffer);
    }

    public static WalRecord deserialize(DataInputStream dis) throws IOException {
        // Read length
        int payloadLength = dis.readInt();
        if (payloadLength <= 0) {
            throw new IOException("Invalid record length: " + payloadLength);
        }

        // Read the rest of the record into a byte array
        byte[] data = new byte[payloadLength];
        dis.readFully(data);

        // Use ByteBuffer to parse the data array
        ByteBuffer buffer = ByteBuffer.wrap(data);

        short partitionId = buffer.getShort();
        long offset = buffer.getLong();

        short keyLength = buffer.getShort();
        if (keyLength < 0) {throw new IOException("Invalid key length: " + keyLength);}
        byte[] key = new byte[keyLength];
        buffer.get(key);

        short valueLength = buffer.getShort();
        if (valueLength < 0) {throw new IOException("Invalid value length: " + valueLength);}
        byte[] value = new byte[valueLength];
        buffer.get(value);

        WalRecord entry = new WalRecord(partitionId, offset, key, value);
        return entry;
    }

    public static WalRecord deserialize(ByteBuffer payloadBuffer) throws IOException {

        short partitionId = payloadBuffer.getShort();

        long offset = payloadBuffer.getLong();

        short keyLength = payloadBuffer.getShort();

        if (keyLength < 0) {
            throw new IOException("Invalid key length: " + keyLength);
        }

        byte[] key = new byte[keyLength];
        payloadBuffer.get(key);

        short valueLength = payloadBuffer.getShort();

        if (valueLength < 0) {
            throw new IOException("Invalid value length: " + valueLength);
        }

        byte[] value = new byte[valueLength];
        payloadBuffer.get(value);

        WalRecord entry = new WalRecord(partitionId, offset, key, value);

        return entry;
    }

    @Override
    public String toString() {
        return String.format(
                "WalRecord[Partition: %d, Offset: %d, Key: \"%s\", Value: \"%s\"]",
                partitionId,
                offset,
                new String(key, StandardCharsets.UTF_8),
                new String(value, StandardCharsets.UTF_8)
        );
    }
}
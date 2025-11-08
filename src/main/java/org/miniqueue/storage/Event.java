package org.miniqueue.storage;

import java.nio.ByteBuffer;

/**
 * Represents actual message [offset+key+value] that gets stored in the
 * queue. This record is what gets put in the data file. There is a simlar record
 * which gets written to WAL. Check WalRecord
 * TODO : Add a CRC at the end to validate data integrity.
 */
public class Event implements Record {

    private final long offset;
    private final byte[] key;
    private final byte[] value;

    public Event(long offset, byte[] key, byte[] value) {
        if (key.length > Short.MAX_VALUE || value.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Key or Value exceeds 32KB limit.");
        }
        this.offset = offset;
        this.key = key;
        this.value = value;
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


    public int getSizeInBytes() {
        // Offset + KeyLen + Key + ValLen + Val
        return 8 + 2 + key.length + 2 + value.length;
    }


    public void serialize(ByteBuffer buffer) {
        // [Offset (long - 8 bytes)]
        buffer.putLong(offset);

        // [KeyLength (short - 2 bytes)]
        buffer.putShort((short) key.length);
        // [KeyData (bytes - variable size)]
        buffer.put(key);

        // [ValueLength (short - 2 bytes)]
        buffer.putShort((short) value.length);
        // [ValueData (bytes - variable size)]
        buffer.put(value);
    }

    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(getSizeInBytes());
        serialize(buffer);
        buffer.flip(); // flip it so it can be read again
        return buffer;
    }


    public static Event deserialize(ByteBuffer buffer) {
        long offset = buffer.getLong(); // 8 byte offset

        short keyLength = buffer.getShort(); // key len
        byte[] key = new byte[keyLength]; // allocate buffer large enough to read bytes
        buffer.get(key);

        short valueLength = buffer.getShort(); // value len
        byte[] value = new byte[valueLength];
        buffer.get(value);
        return new Event(offset, key, value);
    }
}

package org.lsmtdb.core.sstable.util;

import java.nio.ByteBuffer;

public class SSTableEntryHeader {
    public final int keyLength;
    public final int valueLength;
    public final long timestamp;

    public SSTableEntryHeader(int keyLength, int valueLength, long timestamp) {
        this.keyLength = keyLength;
        this.valueLength = valueLength;
        this.timestamp = timestamp;
    }

    public static SSTableEntryHeader readFrom(ByteBuffer buffer) {
        int keyLength = buffer.getInt();
        int valueLength = buffer.getInt();
        long timestamp = buffer.getLong();
        return new SSTableEntryHeader(keyLength, valueLength, timestamp);
    }

    public static void writeTo(ByteBuffer buffer, int keyLength, int valueLength, long timestamp) {
        buffer.putInt(keyLength);
        buffer.putInt(valueLength);
        buffer.putLong(timestamp);
    }
} 
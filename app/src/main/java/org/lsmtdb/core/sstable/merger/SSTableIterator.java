package org.lsmtdb.core.sstable.merger;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.SSTableReader;
import org.lsmtdb.core.sstable.util.SSTableConstants;
import org.lsmtdb.core.sstable.util.SSTableEntryHeader;

public class SSTableIterator implements Comparable<SSTableIterator> {
    private final SSTableReader reader;
    private long currentOffset;
    private ByteArrayWrapper currentKey;
    private byte[] currentValue;
    private boolean hasNext;
    private long currentTimestamp;

    public SSTableIterator(SSTableReader reader) throws IOException {
        this.reader = reader;
        this.currentOffset = reader.getDataOffset();
        this.hasNext = true;
    }

    public boolean hasNext() {
        return hasNext;
    }

    public void next() throws IOException {
        if (!hasNext) {
            throw new IllegalStateException("no more elements");
        }
        advance();
    }

    private void advance() throws IOException {
        if (currentOffset >= reader.getIndexOffset()) {
            hasNext = false;
            return;
        }

        SSTableEntryHeader header = reader.readEntryHeader(currentOffset);
        currentOffset += SSTableConstants.HEADER_SIZE;
        byte[] key = reader.readBytes(currentOffset, header.keyLength);
        currentOffset += header.keyLength;
        currentKey = new ByteArrayWrapper(key);
        currentTimestamp = header.timestamp;

        if (header.valueLength == -1) {
            currentValue = null;
        } else if (header.valueLength > 0) {
            currentValue = reader.readBytes(currentOffset, header.valueLength);
            currentOffset += header.valueLength;
        } else {
            currentValue = new byte[0];
        }
        currentKey = new ByteArrayWrapper(key);
    }

    // private SSTableEntryHeader readEntryHeader(long offset) throws IOException {
    //     ByteBuffer headerBuffer = ByteBuffer.allocate(SSTableConstants.HEADER_SIZE);
    //     int bytesRead = reader.getChannel().read(headerBuffer, currentOffset);
    //     if (bytesRead < SSTableConstants.HEADER_SIZE) {
    //         return null;
    //     }
    //     headerBuffer.flip();
    //     return SSTableEntryHeader.readFrom(headerBuffer);
    // }

    // private byte[] readBytes(long offset, int byteLength) throws IOException {
    //     if (byteLength < 0) {
    //         throw new IOException("invalid byte length for read: " + byteLength);
    //     }
    //     if (byteLength == 0) {
    //         return new byte[0];
    //     }
    //     ByteBuffer tempBuffer = ByteBuffer.allocate(byteLength);
    //     reader.getChannel().read(tempBuffer, offset);
    //     tempBuffer.flip();
    //     byte[] byteArray = new byte[byteLength];
    //     tempBuffer.get(byteArray);
    //     return byteArray;
    // }

    public ByteArrayWrapper getCurrentKey() {
        return currentKey;
    }

    public byte[] getCurrentValue() {
        return currentValue;
    }

    public long getCurrentTimestamp() {
        return currentTimestamp;
    }

    @Override
    public int compareTo(SSTableIterator other) {
        return currentKey.compareTo(other.getCurrentKey());
    }
}

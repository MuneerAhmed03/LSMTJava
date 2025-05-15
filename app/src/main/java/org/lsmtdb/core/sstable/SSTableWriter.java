package org.lsmtdb.core.sstable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.*;

public class SSTableWriter implements AutoCloseable {
    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final int INDEX_ENTRY_INTERVAL = 128;
    private static final long FOOTER_MAGIC = 0xFACEDBEECAFEBEEFL;

    private final FileChannel channel;
    private long currentOffset;
    private final List<IndexEntry> index;
    private final ByteBuffer buffer;
    private boolean isClosed;

    static class IndexEntry {
        private final byte[] key;
        private final long offset;

        IndexEntry(byte[] key, long offset) {
            this.key = key;
            this.offset = offset;
        }
    }

    public SSTableWriter(String filepath) throws IOException {
        File file = new File(filepath);
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.currentOffset = 0;
        this.index = new ArrayList<>();
        this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.isClosed = false;
    }

    public void write(Memtable memtable) throws IOException {
        if (isClosed) {
            throw new IllegalStateException("SSTableWriter is already closed");
        }
        long dataOffset = currentOffset;
        writeData(memtable);
        long indexOffset = currentOffset;
        writeIndex();
        writeFooter(indexOffset,dataOffset);
    }


    private void writeData(Memtable memtable) throws IOException {
        Iterator<Map.Entry<ByteArrayWrapper, Value>> it = memtable.iterator();

        while (it.hasNext()) {
            long entryOffset = currentOffset + buffer.position();
            Map.Entry<ByteArrayWrapper, Value> entry = it.next();
            writeEntry(entry, entryOffset);
        }

        flushBuffer();
    }

    private void writeEntry(Map.Entry<ByteArrayWrapper, Value> entry, long entryOffset) throws IOException {
        byte[] key = entry.getKey().getData();
        Value value = entry.getValue();

        int keyLength = key.length;
        int valueLength = value.isDeleted() ? 0 : value.getValue().length;
        int entrySize = calculateEntrySize(keyLength, valueLength, value.isDeleted());

        if (buffer.remaining() < entrySize) {
            flushBuffer();
        }

        if (shouldAddIndexEntry()) {
            index.add(new IndexEntry(key, entryOffset));
        }

        writeEntryToBuffer(key, value);
    }

    private int calculateEntrySize(int keyLength, int valueLength, boolean isDeleted) {
        return Integer.BYTES + keyLength + Long.BYTES + Byte.BYTES + 
               (isDeleted ? 0 : Integer.BYTES + valueLength);
    }

    private boolean shouldAddIndexEntry() {
        return index.isEmpty() || index.size() % INDEX_ENTRY_INTERVAL == 0;
    }

    private void writeEntryToBuffer(byte[] key, Value value) {
        buffer.putInt(key.length);
        buffer.putInt(value.isDeleted() ? value.getValue().length : -1);
        buffer.putLong(value.getTimestamp());
        buffer.put(key);
        if (!value.isDeleted()) {
            buffer.put(value.getValue());
        }
    }

    private void writeIndex() throws IOException {
        ByteBuffer indexBuffer = createIndexBuffer();
        channel.write(indexBuffer, currentOffset);
        currentOffset += indexBuffer.position();
    }

    private ByteBuffer createIndexBuffer() {
        int indexSize = calculateIndexSize();
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        indexBuffer.putInt(indexSize);
        
        for (IndexEntry idx : index) {
            indexBuffer.putInt(idx.key.length);
            indexBuffer.put(idx.key);
            indexBuffer.putLong(idx.offset);
        }
        
        indexBuffer.flip();
        return indexBuffer;
    }

    private int calculateIndexSize() {
        int size = Integer.BYTES;
        for (IndexEntry idx : index) {
            size += Integer.BYTES + idx.key.length + Long.BYTES;
        }
        return size;
    }

    private void writeFooter(long indexOffset,long dataOffset) throws IOException {
        ByteBuffer footerBuffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
        footerBuffer.putLong(indexOffset);
        footerBuffer.putLong(dataOffset);
        footerBuffer.putLong(FOOTER_MAGIC);
        footerBuffer.flip();
        channel.write(footerBuffer, currentOffset);
    }

    private void flushBuffer() throws IOException {
        buffer.flip();
        channel.write(buffer, currentOffset);
        currentOffset += buffer.position();
        buffer.clear();
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            channel.close();
            isClosed = true;
        }
    }
}

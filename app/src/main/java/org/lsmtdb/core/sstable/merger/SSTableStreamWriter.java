package org.lsmtdb.core.sstable.merger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import org.lsmtdb.core.sstable.SSTableWriter;
import org.lsmtdb.core.sstable.util.SSTableConstants;
import org.lsmtdb.core.sstable.util.SSTableEntryHeader;
import org.lsmtdb.core.sstable.util.SSTableIndexUtils;
import org.lsmtdb.core.sstable.util.SSTableFooterUtils;

public class SSTableStreamWriter implements AutoCloseable {
    private static final int BUFFER_SIZE = 1024 * 1024; 
    private static final int INDEX_ENTRY_INTERVAL = 128;

    private final FileChannel channel;
    private final ByteBuffer buffer;
    private long currentOffset;
    private final List<SSTableWriter.IndexEntry> index;
    private boolean isClosed;

    public SSTableStreamWriter(String filepath) throws IOException {
        File file = new File(filepath);
        if (!file.exists()) {
            file.createNewFile();
        }
        
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.currentOffset = 0;
        this.index = new ArrayList<>();
        this.isClosed = false;
    }

    public void writeEntry(byte[] key, byte[] value, long timestamp) throws IOException {
        if (isClosed) {
            throw new IllegalStateException("writer is already closed");
        }

        long entryOffset = currentOffset + buffer.position();
        writeEntryToBuffer(key, value, timestamp, entryOffset);

        if (buffer.remaining() < SSTableConstants.HEADER_SIZE) {
            flushBuffer();
        }
    }

    private void writeEntryToBuffer(byte[] key, byte[] value, long timestamp, long entryOffset) {
        int keyLength = key.length;
        int valueLength = value != null ? value.length : -1;
        int entrySize = SSTableConstants.HEADER_SIZE + keyLength + valueLength;
        
        if (buffer.remaining() < entrySize) {
            throw new IllegalStateException("Buffer too small for entry");
        }
        
        if (shouldAddIndexEntry()) {
            index.add(new SSTableWriter.IndexEntry(key, entryOffset));
        }
        
        SSTableEntryHeader.writeTo(buffer, keyLength, valueLength, timestamp);
        buffer.put(key);
        if (value != null) {
            buffer.put(value);
        }
    }

    private boolean shouldAddIndexEntry() {
        return index.isEmpty() || index.size() % INDEX_ENTRY_INTERVAL == 0;
    }

    public void finish() throws IOException {
        if (isClosed) {
            throw new IllegalStateException("writer is already closed");
        }

        // flush any remaining data
        if (buffer.position() > 0) {
            flushBuffer();
        }
        
        // write index
        long indexOffset = currentOffset;
        writeIndex();
        currentOffset += calculateIndexSize();
        
        // write footer
        writeFooter(indexOffset, 0);
    }

    private void flushBuffer() throws IOException {
        buffer.flip();
        channel.write(buffer, currentOffset);
        currentOffset += buffer.limit();
        buffer.clear();
    }

    private void writeIndex() throws IOException {
        int indexSize = calculateIndexSize();
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        SSTableIndexUtils.writeIndex(indexBuffer, index);
        indexBuffer.flip();
        channel.write(indexBuffer, currentOffset);
    }

    private int calculateIndexSize() {
        int size = Integer.BYTES;
        for (SSTableWriter.IndexEntry idx : index) {
            size += Integer.BYTES + idx.getKey().length + Long.BYTES;
        }
        return size;
    }

    private void writeFooter(long indexOffset, long dataOffset) throws IOException {
        ByteBuffer footerBuffer = ByteBuffer.allocate(SSTableConstants.FOOTER_SIZE);
        SSTableFooterUtils.writeFooter(footerBuffer, indexOffset, dataOffset);
        footerBuffer.flip();
        channel.write(footerBuffer, currentOffset);
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            channel.close();
            isClosed = true;
        }
    }
} 
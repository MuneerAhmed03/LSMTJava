package org.lsmtdb.core.sstable.merger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
        Path path = Paths.get(filepath);
        Path parent = path.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("[stream-writer] opened file: " + filepath);
        this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.currentOffset = 0;
        this.index = new ArrayList<>();
        this.isClosed = false;
    }

    public void writeEntry(byte[] key, byte[] value, long timestamp) throws IOException {
        if (isClosed) throw new IllegalStateException("writer is already closed");
        long entryOffset = currentOffset + buffer.position();
        // System.out.println("[stream-writer] writing entry at offset: " + entryOffset + ", key.length=" + key.length + ", value.length=" + (value == null ? -1 : value.length));
        writeEntryToBuffer(key, value, timestamp, entryOffset);
        if (buffer.remaining() < SSTableConstants.HEADER_SIZE) {
            flushBuffer();
        }
    }

    private void writeEntryToBuffer(byte[] key, byte[] value, long timestamp, long entryOffset) throws IOException {
        int keyLength = key.length;
        int valueLength = value != null ? value.length : -1;
        int entrySize = SSTableConstants.HEADER_SIZE + keyLength + (valueLength > 0 ? valueLength : 0);

        if (buffer.remaining() < entrySize) {
            flushBuffer();
        }

        if (shouldAddIndexEntry()) {
            index.add(new SSTableWriter.IndexEntry(key, entryOffset));
        }

        SSTableEntryHeader.writeTo(buffer, keyLength, valueLength, timestamp);
        buffer.put(key);
        if (value != null) buffer.put(value);
    }

    private boolean shouldAddIndexEntry() {
        return index.isEmpty() || index.size() % INDEX_ENTRY_INTERVAL == 0;
    }

    public void finish() throws IOException {
        if (isClosed) throw new IllegalStateException("writer is already closed");
        if (buffer.position() > 0) {
            flushBuffer();
        }
        long indexOffset = currentOffset;
        System.out.println("[stream-writer] writing index at offset: " + indexOffset);
        writeIndex();
        long footerOffset = channel.position();
        System.out.println("[stream-writer] writing footer at offset: " + footerOffset + ", indexOffset=" + indexOffset + ", dataOffset=0");
        writeFooter(indexOffset, 0);
        long fileSize = channel.size();
        System.out.println("[stream-writer] finish complete, file size: " + fileSize + ", footerOffset: " + footerOffset);
    }

    private void flushBuffer() throws IOException {
        if (buffer.position() == 0) return;
        System.out.println("[stream-writer] flushing buffer at file offset: " + currentOffset + ", buffer size: " + buffer.position());
        buffer.flip();
        while (buffer.hasRemaining()) {
            int written =channel.write(buffer, currentOffset);
            currentOffset += written;
        }
        buffer.clear();
        System.out.println("[stream-writer] buffer flushed, new file offset: " + currentOffset);
    }

    private void writeIndex() throws IOException {
        int indexSize = calculateIndexSize();
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        SSTableIndexUtils.writeIndex(indexBuffer, index);
        indexBuffer.flip();

        while (indexBuffer.hasRemaining()) {
            int written = channel.write(indexBuffer, currentOffset);
            currentOffset += written;
        }
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

        while (footerBuffer.hasRemaining()) {
            int written = channel.write(footerBuffer, currentOffset);
            currentOffset += written;
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            try {
                if (channel.isOpen()) {
                    channel.force(true);
                    System.out.println("[stream-writer] file channel forced to disk");
                }
            } finally {
                channel.close();
                isClosed = true;
                System.out.println("[stream-writer] file channel closed");
            }
        }
    }
}

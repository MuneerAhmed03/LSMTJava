package org.lsmtdb.core.sstable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import java.nio.charset.StandardCharsets;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.*;
import org.lsmtdb.core.sstable.util.SSTableConstants;
import org.lsmtdb.core.sstable.util.SSTableEntryHeader;
import org.lsmtdb.core.sstable.util.SSTableFooterUtils;
import org.lsmtdb.core.sstable.util.SSTableIndexUtils;

public class SSTableWriter implements AutoCloseable {
    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final int INDEX_ENTRY_INTERVAL = 128;

    private FileChannel channel;
    private long currentOffset;
    private final List<SSTableIndexUtils.IndexEntry> index;
    private final ByteBuffer buffer;
    private boolean isClosed;
    private final int level;
    private SSTableMetadata metadata;

    public static class IndexEntry implements SSTableIndexUtils.IndexEntry {
        private final byte[] key;
        private final long offset;

        public IndexEntry(byte[] key, long offset) {
            this.key = key;
            this.offset = offset;
        }
        @Override
        public byte[] getKey() { return key; }
        @Override
        public long getOffset() { return offset; }
    }

    public SSTableWriter(int level) throws IOException {
        this.level = level;
        this.currentOffset = 0;
        this.index = new ArrayList<>();
        this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.isClosed = false;
    }

    public void write(Memtable memtable) throws IOException {
        if (isClosed) {
            throw new IllegalStateException("sstablewriter is already closed");
        }

        ByteArrayWrapper minKey = memtable.minKey();
        ByteArrayWrapper maxKey = memtable.maxKey();

        TableDirectory tableDir = TableDirectory.getInstance();


        String filePath = tableDir.generatePath(level);

        Path path = Paths.get(filePath);
        Path parent = path.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
        
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        
        this.channel = new RandomAccessFile(file, "rw").getChannel();

        long dataOffset = currentOffset;
        writeData(memtable);
        long indexOffset = currentOffset;
        writeIndex();
        writeFooter(indexOffset, dataOffset);
        channel.force(true);
        long fileSize = file.length();
        System.out.println("sstable write complete: path=" + filePath + ", level=" + level + ", fileSize=" + fileSize + ", footerOffset=" + currentOffset);

        this.metadata = tableDir.allocateNewSSTable(level, minKey, maxKey, file.length(),filePath,tableDir.getAndIncrementNextFileNumber());

        tableDir.addSSTable(level, metadata);
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
        int entrySize = SSTableConstants.HEADER_SIZE + keyLength + (value.isDeleted() ? 0 : valueLength);
        if (buffer.remaining() < entrySize) {
            flushBuffer();
        }
        if (shouldAddIndexEntry()) {
            index.add(new IndexEntry(key, entryOffset));
        }
        SSTableEntryHeader.writeTo(buffer, key.length, value.isDeleted() ? -1 : value.getValue().length, value.getTimestamp());
        buffer.put(key);
        if (!value.isDeleted()) {
            buffer.put(value.getValue());
        }
    }

    private boolean shouldAddIndexEntry() {
        return index.isEmpty() || index.size() % INDEX_ENTRY_INTERVAL == 0;
    }

    private void writeIndex() throws IOException {
        int indexSize = calculateIndexSize();
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        SSTableIndexUtils.writeIndex(indexBuffer, index);
        indexBuffer.flip();
        channel.write(indexBuffer, currentOffset);
        currentOffset += indexBuffer.limit();
    }

    private int calculateIndexSize() {
        int size = Integer.BYTES;
        for (SSTableIndexUtils.IndexEntry idx : index) {
            size += Integer.BYTES + idx.getKey().length + Long.BYTES;
        }
        return size;
    }

    private void writeFooter(long indexOffset, long dataOffset) throws IOException {
        System.out.println("about to write footer at offset: " + currentOffset);
        ByteBuffer footerBuffer = ByteBuffer.allocate(SSTableConstants.FOOTER_SIZE);
        SSTableFooterUtils.writeFooter(footerBuffer, indexOffset, dataOffset);
        footerBuffer.flip();
        channel.write(footerBuffer, currentOffset);
        System.out.println("footer written at offset: " + currentOffset);
    }

    private void flushBuffer() throws IOException {
        buffer.flip();
        channel.write(buffer, currentOffset);
        currentOffset += buffer.limit();
        buffer.clear();
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            channel.close();
            isClosed = true;
            System.out.println("sstable file channel closed for level=" + level + ", path=" + (channel != null ? channel.toString() : "null"));
        }
    }
}

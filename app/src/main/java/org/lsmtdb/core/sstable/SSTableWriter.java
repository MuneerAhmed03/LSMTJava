package org.lsmtdb.core.sstable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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
    private int dataEntryCount = 0;

    private SSTableWriter.IndexEntry floorIndexEntryForKey398365 = null;
    private static final String TARGET_KEY = "key398365";


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


        String finalFilePath = tableDir.generatePath(level);

        Path finalPath = Paths.get(finalFilePath);
        Path parent = finalPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        // Write to temporary file first to avoid torn/corrupted final files on crash.
        Path tempPath = Paths.get(finalFilePath + ".tmp");
        File tempFile = tempPath.toFile();
        if (!tempFile.exists()) {
            tempFile.createNewFile();
        }

        this.channel = new RandomAccessFile(tempFile, "rw").getChannel();

        long dataOffset = currentOffset;
        writeData(memtable);
        long indexOffset = currentOffset;
        writeIndex();
        writeFooter(indexOffset, dataOffset);
        channel.force(true); // ensure file contents & metadata are flushed
        long tempFileSize = tempFile.length();
        channel.close(); // close before atomic move (esp. important on Windows / WSL boundary)
        isClosed = true;

        // Atomic move temp -> final
        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);

        // fsync directory to persist the new entry in case of crash
        if (parent != null) {
            try (FileChannel dirChannel = FileChannel.open(parent, StandardOpenOption.READ)) {
                dirChannel.force(true);
            } catch (IOException e) {
                System.err.println("[sstable-writer] directory fsync failed: " + e.getMessage());
            }
        }

        long fileSize = Files.size(finalPath);
        System.out.println("sstable write complete (atomic): path=" + finalFilePath + ", level=" + level + ", fileSize=" + fileSize + ", footerOffset=" + currentOffset);

        if (floorIndexEntryForKey398365 != null) {
            String floorKey = new String(floorIndexEntryForKey398365.getKey(), StandardCharsets.UTF_8);
            System.out.println("[sstable-writer] floor index entry for key398365 => key: "
                + floorKey + ", offset: " + floorIndexEntryForKey398365.getOffset());
        } else {
            System.out.println("[sstable-writer] no floor index entry found for key398365");
        }
        

    this.metadata = tableDir.allocateNewSSTable(level, minKey, maxKey, fileSize, finalFilePath, tableDir.getAndIncrementNextFileNumber());

        tableDir.addSSTable(level, metadata);
    }

    private void writeData(Memtable memtable) throws IOException {
        Iterator<Map.Entry<ByteArrayWrapper, Value>> it = memtable.iterator();
        while (it.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = it.next();
            writeEntry(entry);
        }
        flushBuffer();
    }

    private void writeEntry(Map.Entry<ByteArrayWrapper, Value> entry) throws IOException {
    byte[] key = entry.getKey().getData();
    String keyStr = new String(key, StandardCharsets.UTF_8);
        Value value = entry.getValue();
        int keyLength = key.length;
        int valueLength = value.isDeleted() ? 0 : value.getValue().length;
        int entrySize = SSTableConstants.HEADER_SIZE + keyLength + (value.isDeleted() ? 0 : valueLength);

        if (buffer.remaining() < entrySize) {
            flushBuffer();
        }

        long entryOffset = currentOffset + buffer.position();
        if(keyStr.equals("key398365")){
            System.out.println("writing header for key398365 at offset: " + entryOffset);
        }

        dataEntryCount++;
        if (dataEntryCount == 1 || dataEntryCount % INDEX_ENTRY_INTERVAL == 0) {
            SSTableWriter.IndexEntry entryI = new SSTableWriter.IndexEntry(key, entryOffset);
            index.add(entryI);
            System.out.println("[sstable-writer] added index entry: key=" + keyStr + ", offset=" + entryOffset);
            String currentKeyStr = keyStr;
            if (currentKeyStr.compareTo(TARGET_KEY) <= 0) {
                floorIndexEntryForKey398365 = entryI;
            }
            if (currentKeyStr.equals(TARGET_KEY)) {
                System.out.println("written key398365 to sstable");
            }
        }
        SSTableEntryHeader.writeTo(buffer, key.length, value.isDeleted() ? -1 : value.getValue().length, value.getTimestamp());
        buffer.put(key);
        if (!value.isDeleted()) {
            buffer.put(value.getValue());
        }
    }

    private void writeIndex() throws IOException {
        int indexSize = calculateIndexSize();
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        // debug print for index contents
        System.out.println("[sstable-writer] index entries:");
        // for (SSTableIndexUtils.IndexEntry idx : index) {
        //     System.out.println("  key: " + new String(idx.getKey(), java.nio.charset.StandardCharsets.UTF_8) + ", offset: " + idx.getOffset());
        // }
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

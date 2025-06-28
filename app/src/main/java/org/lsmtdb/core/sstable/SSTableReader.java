package org.lsmtdb.core.sstable;

import java.nio.channels.FileChannel;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.util.SSTableConstants;
import org.lsmtdb.core.sstable.util.SSTableEntryHeader;
import org.lsmtdb.core.sstable.util.SSTableFooterUtils;
import org.lsmtdb.core.sstable.util.SSTableFooterUtils.FooterData;
import org.lsmtdb.core.sstable.util.SSTableIndexUtils;
import java.nio.charset.StandardCharsets;
import org.lsmtdb.core.sstable.NotFoundException;

public class SSTableReader implements AutoCloseable {
    private final FileChannel channel;
    private final TreeMap<ByteArrayWrapper,Long> indexMap = new TreeMap<>();
    private final long dataOffset;
    private final long indexOffset;
    private final int indexSize;
    private final long fileSize;

    public SSTableReader(String filepath) throws IOException {


        File file = new File(filepath);

        this.channel = new RandomAccessFile(file, "r").getChannel(); 
        this.fileSize = channel.size();
        
        if (this.fileSize == 0) {
            this.indexOffset = 0;
            this.dataOffset = 0;
            this.indexSize = 0;
            return;
        }
        
        validateFileSize();
        FooterData footer = readFooter();
        this.indexOffset = footer.indexOffset;
        this.dataOffset = footer.dataOffset;
        validateIndexOffset();
        this.indexSize = readIndexSize();
        validateIndexSize();
        loadIndex();
    }
    
    private void validateFileSize() throws IOException {
        if (this.fileSize < SSTableConstants.FOOTER_SIZE) {
            throw new IOException("sstable file is too small to contain a footer");
        }
    }
    
    private FooterData readFooter() throws IOException {
        ByteBuffer footerBuffer = ByteBuffer.allocate(SSTableConstants.FOOTER_SIZE);
        channel.read(footerBuffer, this.fileSize - SSTableConstants.FOOTER_SIZE);
        footerBuffer.flip();
        return SSTableFooterUtils.readFooter(footerBuffer);
    }
    
    private void validateIndexOffset() throws IOException {
        if (this.indexOffset < 0 || this.indexOffset >= this.fileSize) {
            throw new IOException("invalid index offset in sstable footer");
        }
    }
    
    private int readIndexSize() throws IOException {
        ByteBuffer tempBuffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(tempBuffer, this.indexOffset);
        tempBuffer.flip();
        return tempBuffer.getInt();
    }
    
    private void validateIndexSize() throws IOException {
        if (this.indexSize <= 0 || this.indexOffset + this.indexSize > this.fileSize - SSTableConstants.FOOTER_SIZE) {
            throw new IOException("invalid index size in sstable");
        }
    }

    private void loadIndex() throws IOException {
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        channel.read(indexBuffer, this.indexOffset);
        indexBuffer.flip();
        indexMap.putAll(SSTableIndexUtils.readIndex(indexBuffer, indexSize));
    }

    public byte[] get(byte[] targetKey) throws IOException { 
        System.out.println("checking in sst table for key: " + new String(targetKey, StandardCharsets.UTF_8));
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(targetKey);
        Map.Entry<ByteArrayWrapper,Long> entry = indexMap.floorEntry(keyWrapper);
        if (entry == null) {
            System.out.println("no floor entry found in index map");
            throw new NotFoundException("key not found in sstable");
        }
        System.out.println("found floor entry at offset: " + entry.getValue());
        return scanForKey(entry.getValue(), keyWrapper);
    }
    
    private byte[] scanForKey(long startOffset, ByteArrayWrapper targetKeyWrapper) throws IOException {
        long offset = startOffset;
        while (offset < indexOffset) {
            SSTableEntryHeader header = readEntryHeader(offset);
            if (header == null) {
                System.out.println("failed to read entry header at offset: " + offset);
                throw new NotFoundException("key not found in sstable");
            }
            offset += SSTableConstants.HEADER_SIZE;
            if (isOffsetOutOfBounds(offset, header.keyLength)) {
                System.out.println("key length out of bounds at offset: " + offset);
                throw new NotFoundException("key not found in sstable");
            }
            byte[] key = readBytes(offset, header.keyLength);
            ByteArrayWrapper currentKey = new ByteArrayWrapper(key);
            offset += header.keyLength;
            int comparisonResult = currentKey.compareTo(targetKeyWrapper);
            if (comparisonResult == 0) {
                if (header.valueLength == -1) {
                    System.out.println("found tombstone marker");
                    throw new NotFoundException("key is deleted (tombstone)");
                }
                if (isOffsetOutOfBounds(offset, header.valueLength)) {
                    System.out.println("value length out of bounds at offset: " + offset);
                    throw new NotFoundException("key not found in sstable");
                }
                return readBytes(offset, header.valueLength);
            } else if (comparisonResult > 0) {
                System.out.println("key comparison greater than target, stopping search");
                throw new NotFoundException("key not found in sstable");
            }
            if (header.valueLength > 0) {
                offset += header.valueLength;
            }
        }
        System.out.println("reached end of data section without finding key");
        throw new NotFoundException("key not found in sstable");
    }
    
    private boolean isOffsetOutOfBounds(long offset, int length) {
        return offset + length > this.fileSize - SSTableConstants.FOOTER_SIZE;
    }
    
    public SSTableEntryHeader readEntryHeader(long offset) throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(SSTableConstants.HEADER_SIZE);
        int bytesRead = channel.read(headerBuffer, offset);
        if (bytesRead < SSTableConstants.HEADER_SIZE) {
            return null;
        }
        headerBuffer.flip();
        return SSTableEntryHeader.readFrom(headerBuffer);
    }

    public byte[] readBytes(long offset, int byteLength) throws IOException {
        if (byteLength < 0) {
            throw new IOException("invalid byte length for read: " + byteLength);
        }
        if (byteLength == 0) {
            return new byte[0];
        }
        ByteBuffer tempBuffer = ByteBuffer.allocate(byteLength);
        channel.read(tempBuffer, offset);
        tempBuffer.flip();
        byte[] byteArray = new byte[byteLength];
        tempBuffer.get(byteArray);
        return byteArray;
    }
    
    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    public long getDataOffset() {
        return dataOffset;
    }

    public long getIndexOffset() {
        return indexOffset;
    }

    public FileChannel getChannel() {
        return channel;
    }
}
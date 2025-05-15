package org.lsmtdb.core.sstable;

import java.nio.channels.FileChannel;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import org.lsmtdb.common.ByteArrayWrapper;

public class SSTableReader implements AutoCloseable {
    private static final int FOOTER_SIZE = Long.BYTES * 2 + Long.BYTES; 
    private static final long FOOTER_MAGIC_EXPECTED = 0xFACEDBEECAFEBEEFL;
    private static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES;

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
        if (this.fileSize < FOOTER_SIZE) {
            throw new IOException("SSTable file is too small to contain a footer.");
        }
    }
    
    private FooterData readFooter() throws IOException {
        ByteBuffer footerBuffer = ByteBuffer.allocate(FOOTER_SIZE);
        channel.read(footerBuffer, this.fileSize - FOOTER_SIZE);
        footerBuffer.flip();
        
        long indexOffset = footerBuffer.getLong();
        long dataOffset = footerBuffer.getLong();
        long magic = footerBuffer.getLong();
        
        if (magic != FOOTER_MAGIC_EXPECTED) {
            throw new IOException("Invalid SSTable file: footer magic mismatch.");
        }
        
        return new FooterData(indexOffset, dataOffset);
    }
    
    private void validateIndexOffset() throws IOException {
        if (this.indexOffset < 0 || this.indexOffset >= this.fileSize) {
            throw new IOException("Invalid index offset in SSTable footer.");
        }
    }
    
    private int readIndexSize() throws IOException {
        ByteBuffer tempBuffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(tempBuffer, this.indexOffset);
        tempBuffer.flip();
        return tempBuffer.getInt();
    }
    
    private void validateIndexSize() throws IOException {
        if (this.indexSize <= 0 || this.indexOffset + this.indexSize > this.fileSize - FOOTER_SIZE) {
            throw new IOException("Invalid index size in SSTable.");
        }
    }

    private void loadIndex() throws IOException {
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        channel.read(indexBuffer, this.indexOffset);
        indexBuffer.flip();
        
        indexBuffer.getInt(); 

        while (indexBuffer.hasRemaining()) {
            int keyLength = indexBuffer.getInt();
            byte[] keyBytes = new byte[keyLength];
            indexBuffer.get(keyBytes);
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(keyBytes);
            long offset = indexBuffer.getLong();
            indexMap.put(keyWrapper, offset);
        }
    }

    public byte[] get(byte[] targetKey) throws IOException { 
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(targetKey);
        Map.Entry<ByteArrayWrapper,Long> entry = indexMap.floorEntry(keyWrapper);
        
        if (entry == null) {
            return null;
        }

        return scanForKey(entry.getValue(), keyWrapper);
    }
    
    private byte[] scanForKey(long startOffset, ByteArrayWrapper targetKeyWrapper) throws IOException {
        long offset = startOffset;
        
        while (offset < indexOffset) {
            EntryHeader header = readEntryHeader(offset);
            if (header == null) {
                return null;
            }
            
            offset += HEADER_SIZE;
            
            if (isOffsetOutOfBounds(offset, header.keyLength)) {
                return null;
            }
            
            byte[] key = readBytes(offset, header.keyLength);
            ByteArrayWrapper currentKey = new ByteArrayWrapper(key);
            offset += header.keyLength;
            
            int comparisonResult = currentKey.compareTo(targetKeyWrapper);
            
            if (comparisonResult == 0) {
                if (header.valueLength == -1) return null;
                if (isOffsetOutOfBounds(offset, header.valueLength)) {
                    return null;
                }
                return readBytes(offset, header.valueLength);
            } else if (comparisonResult > 0) {
                return null;
            }
            
            if (header.valueLength > 0) {
                offset += header.valueLength;
            }
        }
        
        return null;
    }
    
    private boolean isOffsetOutOfBounds(long offset, int length) {
        return offset + length > this.fileSize - FOOTER_SIZE;
    }
    
    private EntryHeader readEntryHeader(long offset) throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
        int bytesRead = channel.read(headerBuffer, offset);
        
        if (bytesRead < HEADER_SIZE) {
            return null;
        }
        
        headerBuffer.flip();
        int keyLength = headerBuffer.getInt();
        int valueLength = headerBuffer.getInt();
        long timestamp = headerBuffer.getLong();
        
        return new EntryHeader(keyLength, valueLength, timestamp);
    }

    private byte[] readBytes(long offset, int byteLength) throws IOException {
        if (byteLength < 0) {
            throw new IOException("Invalid byte length for read: " + byteLength);
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
    
    private static class FooterData {
        final long indexOffset;
        final long dataOffset;
        
        FooterData(long indexOffset, long dataOffset) {
            this.indexOffset = indexOffset;
            this.dataOffset = dataOffset;
        }
    }
    
    private static class EntryHeader {
        final int keyLength;
        final int valueLength;
        final long timestamp;
        
        EntryHeader(int keyLength, int valueLength, long timestamp) {
            this.keyLength = keyLength;
            this.valueLength = valueLength;
            this.timestamp = timestamp;
        }
    }
}
package org.lsmtdb.test.utils;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.SSTableReader;
import org.lsmtdb.core.sstable.util.SSTableConstants;
import org.lsmtdb.core.sstable.util.SSTableEntryHeader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Helper class for inspecting SSTable structure and validating correctness.
 * Provides low-level access to SSTable contents for testing purposes.
 */
public class SSTableInspector {
    
    /**
     * Entry representing a key-value-timestamp tuple from an SSTable.
     */
    public static class SSTableEntry {
        public final byte[] key;
        public final byte[] value;
        public final long timestamp;
        public final boolean isDeleted;
        
        public SSTableEntry(byte[] key, byte[] value, long timestamp, boolean isDeleted) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.isDeleted = isDeleted;
        }
        
        public String getKeyAsString() {
            return new String(key, StandardCharsets.UTF_8);
        }
        
        public String getValueAsString() {
            if (value == null || isDeleted) {
                return "<deleted>";
            }
            return new String(value, StandardCharsets.UTF_8);
        }
        
        @Override
        public String toString() {
            return String.format("Entry{key=%s, value=%s, timestamp=%d, deleted=%b}",
                    getKeyAsString(), getValueAsString(), timestamp, isDeleted);
        }
    }
    
    /**
     * Read all keys from an SSTable file.
     * 
     * @param sstablePath Path to the SSTable file
     * @return List of keys in order
     * @throws IOException If reading fails
     */
    public static List<byte[]> readAllKeys(String sstablePath) throws IOException {
        validatePath(sstablePath);
        
        List<byte[]> keys = new ArrayList<>();
        
        try (SSTableReader reader = new SSTableReader(sstablePath)) {
            long offset = reader.getDataOffset();
            long indexOffset = reader.getIndexOffset();
            
            while (offset < indexOffset) {
                SSTableEntryHeader header = reader.readEntryHeader(offset);
                if (header == null) {
                    break;
                }
                
                offset += SSTableConstants.HEADER_SIZE;
                
                byte[] key = reader.readBytes(offset, header.keyLength);
                keys.add(key);
                
                offset += header.keyLength;
                
                if (header.valueLength > 0) {
                    offset += header.valueLength;
                }
            }
        }
        
        return keys;
    }
    
    /**
     * Read all entries (key-value-timestamp) from an SSTable file.
     * 
     * @param sstablePath Path to the SSTable file
     * @return List of SSTableEntry objects
     * @throws IOException If reading fails
     */
    public static List<SSTableEntry> readAllEntries(String sstablePath) throws IOException {
        validatePath(sstablePath);
        
        List<SSTableEntry> entries = new ArrayList<>();
        
        try (SSTableReader reader = new SSTableReader(sstablePath)) {
            long offset = reader.getDataOffset();
            long indexOffset = reader.getIndexOffset();
            
            while (offset < indexOffset) {
                SSTableEntryHeader header = reader.readEntryHeader(offset);
                if (header == null) {
                    break;
                }
                
                offset += SSTableConstants.HEADER_SIZE;
                
                byte[] key = reader.readBytes(offset, header.keyLength);
                offset += header.keyLength;
                
                byte[] value = null;
                boolean isDeleted = (header.valueLength == -1);
                
                if (!isDeleted && header.valueLength > 0) {
                    value = reader.readBytes(offset, header.valueLength);
                    offset += header.valueLength;
                } else if (!isDeleted && header.valueLength == 0) {
                    value = new byte[0];
                }
                
                entries.add(new SSTableEntry(key, value, header.timestamp, isDeleted));
            }
        }
        
        return entries;
    }
    
    /**
     * Verify that all keys in the SSTable are in sorted order.
     * 
     * @param sstablePath Path to the SSTable file
     * @return true if keys are sorted, false otherwise
     * @throws IOException If reading fails
     */
    public static boolean verifyKeyOrdering(String sstablePath) throws IOException {
        List<byte[]> keys = readAllKeys(sstablePath);
        
        if (keys.size() <= 1) {
            return true;
        }
        
        for (int i = 0; i < keys.size() - 1; i++) {
            ByteArrayWrapper current = new ByteArrayWrapper(keys.get(i));
            ByteArrayWrapper next = new ByteArrayWrapper(keys.get(i + 1));
            
            if (current.compareTo(next) >= 0) {
                System.err.println("Key ordering violation at index " + i + ": " +
                        new String(keys.get(i), StandardCharsets.UTF_8) + " >= " +
                        new String(keys.get(i + 1), StandardCharsets.UTF_8));
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Count the total number of entries in the SSTable.
     * 
     * @param sstablePath Path to the SSTable file
     * @return Number of entries
     * @throws IOException If reading fails
     */
    public static int getKeyCount(String sstablePath) throws IOException {
        return readAllKeys(sstablePath).size();
    }
    
    /**
     * Get the minimum and maximum keys in the SSTable.
     * 
     * @param sstablePath Path to the SSTable file
     * @return Array with [minKey, maxKey], or null if SSTable is empty
     * @throws IOException If reading fails
     */
    public static byte[][] getMinMaxKeys(String sstablePath) throws IOException {
        List<byte[]> keys = readAllKeys(sstablePath);
        
        if (keys.isEmpty()) {
            return null;
        }
        
        return new byte[][] { keys.get(0), keys.get(keys.size() - 1) };
    }
    
    /**
     * Check if a specific key exists in the SSTable.
     * 
     * @param sstablePath Path to the SSTable file
     * @param targetKey Key to search for
     * @return true if key exists, false otherwise
     * @throws IOException If reading fails
     */
    public static boolean hasKey(String sstablePath, byte[] targetKey) throws IOException {
        if (targetKey == null) {
            throw new IllegalArgumentException("Target key cannot be null");
        }
        
        ByteArrayWrapper target = new ByteArrayWrapper(targetKey);
        List<byte[]> keys = readAllKeys(sstablePath);
        
        for (byte[] key : keys) {
            if (target.equals(new ByteArrayWrapper(key))) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Print all contents of the SSTable for debugging.
     * 
     * @param sstablePath Path to the SSTable file
     * @throws IOException If reading fails
     */
    public static void dumpContents(String sstablePath) throws IOException {
        System.out.println("=== SSTable Contents: " + sstablePath + " ===");
        
        File file = new File(sstablePath);
        System.out.println("File size: " + file.length() + " bytes");
        
        List<SSTableEntry> entries = readAllEntries(sstablePath);
        System.out.println("Total entries: " + entries.size());
        System.out.println();
        
        for (int i = 0; i < entries.size(); i++) {
            SSTableEntry entry = entries.get(i);
            System.out.println("[" + i + "] " + entry);
        }
        
        System.out.println("=== End of SSTable ===");
    }
    
    /**
     * Validate SSTable metadata consistency.
     * Checks that the file structure is valid and metadata matches actual content.
     * 
     * @param sstablePath Path to the SSTable file
     * @return true if metadata is valid, false otherwise
     * @throws IOException If reading fails
     */
    public static boolean validateMetadata(String sstablePath) throws IOException {
        validatePath(sstablePath);
        
        File file = new File(sstablePath);
        
        if (!file.exists()) {
            System.err.println("SSTable file does not exist: " + sstablePath);
            return false;
        }
        
        if (file.length() == 0) {
            System.err.println("SSTable file is empty: " + sstablePath);
            return false;
        }
        
        try (SSTableReader reader = new SSTableReader(sstablePath)) {
            // Try to read all entries - if this succeeds, structure is valid
            List<SSTableEntry> entries = readAllEntries(sstablePath);
            
            // Verify key ordering
            if (!verifyKeyOrdering(sstablePath)) {
                System.err.println("Key ordering validation failed");
                return false;
            }
            
            return true;
        } catch (Exception e) {
            System.err.println("Metadata validation failed: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Validate that an SSTable path exists and is a file.
     * 
     * @param sstablePath Path to validate
     * @throws IOException If path is invalid
     */
    private static void validatePath(String sstablePath) throws IOException {
        if (sstablePath == null || sstablePath.trim().isEmpty()) {
            throw new IllegalArgumentException("SSTable path cannot be null or empty");
        }
        
        File file = new File(sstablePath);
        if (!file.exists()) {
            throw new IOException("SSTable file does not exist: " + sstablePath);
        }
        
        if (!file.isFile()) {
            throw new IOException("SSTable path is not a file: " + sstablePath);
        }
    }
    
    /**
     * Get a statistical summary of the SSTable.
     * 
     * @param sstablePath Path to the SSTable file
     * @return Map containing statistics
     * @throws IOException If reading fails
     */
    public static Map<String, Object> getStatistics(String sstablePath) throws IOException {
        Map<String, Object> stats = new HashMap<>();
        
        File file = new File(sstablePath);
        stats.put("filePath", sstablePath);
        stats.put("fileSize", file.length());
        
        List<SSTableEntry> entries = readAllEntries(sstablePath);
        stats.put("entryCount", entries.size());
        
        if (!entries.isEmpty()) {
            byte[][] minMax = getMinMaxKeys(sstablePath);
            stats.put("minKey", new String(minMax[0], StandardCharsets.UTF_8));
            stats.put("maxKey", new String(minMax[1], StandardCharsets.UTF_8));
            
            long deletedCount = entries.stream().filter(e -> e.isDeleted).count();
            stats.put("deletedCount", deletedCount);
            stats.put("activeCount", entries.size() - deletedCount);
        }
        
        stats.put("isOrdered", verifyKeyOrdering(sstablePath));
        
        return stats;
    }
}

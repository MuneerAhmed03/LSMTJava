package org.lsmtdb.test.utils;

import org.lsmtdb.common.ByteArrayWrapper;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Helper class for generating deterministic test data for LSM Tree tests.
 * All methods produce consistent output given the same inputs, making tests reproducible.
 */
public class TestDataGenerator {
    
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;
    
    /**
     * Generate a byte[] key from an integer ID.
     * Format: "key_00001" (zero-padded to 5 digits)
     * 
     * @param id The integer ID to convert to a key
     * @return Byte array representation of the key
     */
    public static byte[] generateKey(int id) {
        return String.format("key_%05d", id).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Generate a byte[] value from an integer ID.
     * Format: "value_00001" (zero-padded to 5 digits)
     * 
     * @param id The integer ID to convert to a value
     * @return Byte array representation of the value
     */
    public static byte[] generateValue(int id) {
        return String.format("value_%05d", id).getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Generate a list of keys in the specified range [start, end).
     * 
     * @param start Starting ID (inclusive)
     * @param end Ending ID (exclusive)
     * @return List of byte[] keys
     */
    public static List<byte[]> generateKeyRange(int start, int end) {
        if (start >= end) {
            throw new IllegalArgumentException("Start must be less than end");
        }
        
        List<byte[]> keys = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            keys.add(generateKey(i));
        }
        return keys;
    }
    
    /**
     * Generate a map of key-value pairs with the specified count.
     * Keys and values are generated sequentially starting from ID 0.
     * 
     * @param count Number of key-value pairs to generate
     * @return Map of ByteArrayWrapper keys to byte[] values
     */
    public static Map<ByteArrayWrapper, byte[]> generateKeyValuePairs(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("Count must be non-negative");
        }
        
        Map<ByteArrayWrapper, byte[]> pairs = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            pairs.put(createByteArrayWrapper(i), generateValue(i));
        }
        return pairs;
    }
    
    /**
     * Generate a deterministic timestamp based on an offset.
     * Base timestamp + offset milliseconds.
     * 
     * @param offset Offset in milliseconds from base timestamp
     * @return Timestamp value
     */
    public static long generateTimestamp(int offset) {
        return BASE_TIMESTAMP + offset;
    }
    
    /**
     * Create a ByteArrayWrapper from a string.
     * 
     * @param s String to wrap
     * @return ByteArrayWrapper instance
     */
    public static ByteArrayWrapper createByteArrayWrapper(String s) {
        if (s == null) {
            throw new IllegalArgumentException("String cannot be null");
        }
        return new ByteArrayWrapper(s.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Create a ByteArrayWrapper from an integer ID.
     * Uses the standard key format "key_00001".
     * 
     * @param id Integer ID to convert
     * @return ByteArrayWrapper instance
     */
    public static ByteArrayWrapper createByteArrayWrapper(int id) {
        return new ByteArrayWrapper(generateKey(id));
    }
    
    /**
     * Generate a random byte array of specified length.
     * Uses a seeded random for reproducibility.
     * 
     * @param length Length of the byte array
     * @param seed Random seed for reproducibility
     * @return Byte array with random data
     */
    public static byte[] generateRandomBytes(int length, long seed) {
        if (length < 0) {
            throw new IllegalArgumentException("Length must be non-negative");
        }
        
        Random random = new Random(seed);
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
    
    /**
     * Generate a large value for testing size limits.
     * 
     * @param sizeInBytes Size of the value in bytes
     * @param id ID to use for pattern (repeated throughout)
     * @return Byte array of specified size
     */
    public static byte[] generateLargeValue(int sizeInBytes, int id) {
        if (sizeInBytes < 0) {
            throw new IllegalArgumentException("Size must be non-negative");
        }
        
        String pattern = String.format("value_%05d_", id);
        byte[] patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[sizeInBytes];
        
        for (int i = 0; i < sizeInBytes; i++) {
            result[i] = patternBytes[i % patternBytes.length];
        }
        
        return result;
    }
}

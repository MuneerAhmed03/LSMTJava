package org.lsmtdb.test.utils;

import org.junit.jupiter.api.Test;
import org.lsmtdb.common.ByteArrayWrapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Example tests demonstrating the usage of test utility classes.
 * These tests serve as documentation and validation of the utilities.
 */
public class TestUtilitiesExampleTest {
    
    @Test
    public void testDataGeneratorBasicUsage() {
        // Generate keys and values
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        
        assertEquals("key_00001", new String(key, StandardCharsets.UTF_8));
        assertEquals("value_00001", new String(value, StandardCharsets.UTF_8));
    }
    
    @Test
    public void testDataGeneratorKeyRange() {
        // Generate a range of keys
        List<byte[]> keys = TestDataGenerator.generateKeyRange(0, 5);
        
        assertEquals(5, keys.size());
        assertEquals("key_00000", new String(keys.get(0), StandardCharsets.UTF_8));
        assertEquals("key_00004", new String(keys.get(4), StandardCharsets.UTF_8));
    }
    
    @Test
    public void testDataGeneratorKeyValuePairs() {
        // Generate key-value pairs
        Map<ByteArrayWrapper, byte[]> pairs = TestDataGenerator.generateKeyValuePairs(10);
        
        assertEquals(10, pairs.size());
        
        // Verify they're sorted (TreeMap guarantees this)
        List<ByteArrayWrapper> keys = pairs.keySet().stream().toList();
        for (int i = 0; i < keys.size() - 1; i++) {
            assertTrue(keys.get(i).compareTo(keys.get(i + 1)) < 0);
        }
    }
    
    @Test
    public void testDataGeneratorTimestamp() {
        long ts1 = TestDataGenerator.generateTimestamp(0);
        long ts2 = TestDataGenerator.generateTimestamp(100);
        
        assertEquals(100, ts2 - ts1);
    }
    
    @Test
    public void testDataGeneratorByteArrayWrapper() {
        ByteArrayWrapper wrapper1 = TestDataGenerator.createByteArrayWrapper("test");
        ByteArrayWrapper wrapper2 = TestDataGenerator.createByteArrayWrapper(42);
        
        assertNotNull(wrapper1);
        assertNotNull(wrapper2);
        assertEquals("test", new String(wrapper1.getData(), StandardCharsets.UTF_8));
        assertEquals("key_00042", new String(wrapper2.getData(), StandardCharsets.UTF_8));
    }
    
    @Test
    public void testDataGeneratorLargeValue() {
        byte[] largeValue = TestDataGenerator.generateLargeValue(1000, 5);
        
        assertEquals(1000, largeValue.length);
        
        // Check pattern repetition
        String pattern = "value_00005_";
        byte[] patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 100; i++) {
            assertEquals(patternBytes[i % patternBytes.length], largeValue[i]);
        }
    }
    
    @Test
    public void testDataGeneratorRandomBytes() {
        byte[] random1 = TestDataGenerator.generateRandomBytes(100, 12345L);
        byte[] random2 = TestDataGenerator.generateRandomBytes(100, 12345L);
        byte[] random3 = TestDataGenerator.generateRandomBytes(100, 67890L);
        
        // Same seed produces same output
        assertArrayEquals(random1, random2);
        
        // Different seed produces different output (with very high probability)
        assertFalse(java.util.Arrays.equals(random1, random3));
    }
    
    @Test
    public void testInvariantCheckerCreation() {
        // Test that InvariantChecker can be created
        InvariantChecker checker = new InvariantChecker();
        assertNotNull(checker);
        
        // Initially no violations
        assertEquals(0, checker.getViolations().size());
    }
    
    @Test
    public void testInvariantCheckerViolationReport() {
        InvariantChecker checker = new InvariantChecker();
        
        List<String> report = checker.reportViolations();
        assertNotNull(report);
        assertTrue(report.size() > 0);
        assertTrue(report.get(0).contains("No invariant violations"));
    }
}

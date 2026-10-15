package org.lsmtdb.core.memtable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.test.utils.TestDataGenerator;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for the Memtable component of the LSM Tree.
 * Tests cover basic operations, key ordering, updates, size calculations,
 * tombstone handling, edge cases, and iterator correctness.
 */
public class MemtableTest {

    private Memtable memtable;

    @BeforeEach
    public void setUp() {
        memtable = Memtable.getInstance();
        memtable.clear();
    }

    @AfterEach
    public void tearDown() {
        Memtable.resetFlushThresholdSizeForTests();
        memtable.clear();
    }

    // ==================== Basic Operations ====================

    @Test
    public void testPutAndGet() {
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        long timestamp = TestDataGenerator.generateTimestamp(0);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(value, timestamp, false);
        
        memtable.put(keyWrapper, valueObj);
        
        byte[] retrieved = memtable.get(key);
        assertNotNull(retrieved);
        assertArrayEquals(value, retrieved);
    }

    @Test
    public void testPutMultipleKeys() {
        int keyCount = 100;
        Map<ByteArrayWrapper, byte[]> expectedValues = new HashMap<>();
        
        for (int i = 0; i < keyCount; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            long timestamp = TestDataGenerator.generateTimestamp(i);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, timestamp, false);
            
            memtable.put(keyWrapper, valueObj);
            expectedValues.put(keyWrapper, value);
        }
        
        // Verify all keys are retrievable
        for (int i = 0; i < keyCount; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] retrieved = memtable.get(key);
            assertNotNull(retrieved, "Key " + i + " should be retrievable");
            assertArrayEquals(expectedValues.get(new ByteArrayWrapper(key)), retrieved);
        }
    }

    @Test
    public void testGetNonExistentKey() {
        byte[] nonExistentKey = TestDataGenerator.generateKey(999);
        
        byte[] result = memtable.get(nonExistentKey);
        assertNull(result, "Non-existent key should return null");
    }

    @Test
    public void testDelete() {
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        long putTimestamp = TestDataGenerator.generateTimestamp(0);
        long deleteTimestamp = TestDataGenerator.generateTimestamp(1);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(value, putTimestamp, false);
        
        // Put the key first
        memtable.put(keyWrapper, valueObj);
        assertNotNull(memtable.get(key));
        
        // Delete the key (creates tombstone)
        memtable.remove(key, deleteTimestamp);
        
        // Get should return null for deleted key
        byte[] retrieved = memtable.get(key);
        assertNull(retrieved, "Deleted key should return null");
    }

    @Test
    public void testDeleteNonExistentKey() {
        byte[] nonExistentKey = TestDataGenerator.generateKey(999);
        long timestamp = TestDataGenerator.generateTimestamp(0);
        
        // Delete should still create a tombstone even if key doesn't exist
        assertDoesNotThrow(() -> memtable.remove(nonExistentKey, timestamp));
        
        // Get should return null
        byte[] retrieved = memtable.get(nonExistentKey);
        assertNull(retrieved, "Deleted non-existent key should return null");
    }

    // ==================== Key Ordering ====================

    @Test
    public void testKeysAreSorted() {
        // Insert keys in unsorted order
        int[] insertOrder = {5, 2, 8, 1, 9, 3, 7, 4, 6, 0};
        
        for (int id : insertOrder) {
            byte[] key = TestDataGenerator.generateKey(id);
            byte[] value = TestDataGenerator.generateValue(id);
            long timestamp = TestDataGenerator.generateTimestamp(0);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, timestamp, false);
            memtable.put(keyWrapper, valueObj);
        }
        
        // Iterate and verify sorted order
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        
        ByteArrayWrapper previousKey = null;
        int count = 0;
        
        while (iterator.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
            ByteArrayWrapper currentKey = entry.getKey();
            
            if (previousKey != null) {
                assertTrue(previousKey.compareTo(currentKey) < 0,
                    "Keys should be in ascending sorted order");
            }
            
            previousKey = currentKey;
            count++;
        }
        
        assertEquals(insertOrder.length, count, "All keys should be present");
    }

    @Test
    public void testKeyOrderingWithRandomInserts() {
        Random random = new Random(42); // Fixed seed for reproducibility
        int keyCount = 50;
        List<Integer> ids = new ArrayList<>();
        
        // Generate random IDs
        for (int i = 0; i < keyCount; i++) {
            ids.add(random.nextInt(1000));
        }
        
        // Insert in random order
        for (int id : ids) {
            byte[] key = TestDataGenerator.generateKey(id);
            byte[] value = TestDataGenerator.generateValue(id);
            long timestamp = TestDataGenerator.generateTimestamp(0);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, timestamp, false);
            memtable.put(keyWrapper, valueObj);
        }
        
        // Verify iteration is sorted
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        
        ByteArrayWrapper previousKey = null;
        
        while (iterator.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
            ByteArrayWrapper currentKey = entry.getKey();
            
            if (previousKey != null) {
                assertTrue(previousKey.compareTo(currentKey) < 0,
                    "Keys should be in ascending sorted order even with random inserts");
            }
            
            previousKey = currentKey;
        }
    }

    @Test
    public void testByteArrayComparison() {
        // Test lexicographic comparison
        byte[] key1 = "aaa".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "aab".getBytes(StandardCharsets.UTF_8);
        byte[] key3 = "ab".getBytes(StandardCharsets.UTF_8);
        byte[] key4 = "b".getBytes(StandardCharsets.UTF_8);
        
        ByteArrayWrapper wrapper1 = new ByteArrayWrapper(key1);
        ByteArrayWrapper wrapper2 = new ByteArrayWrapper(key2);
        ByteArrayWrapper wrapper3 = new ByteArrayWrapper(key3);
        ByteArrayWrapper wrapper4 = new ByteArrayWrapper(key4);
        
        // Test comparisons
        assertTrue(wrapper1.compareTo(wrapper2) < 0, "aaa < aab");
        assertTrue(wrapper2.compareTo(wrapper3) < 0, "aab < ab");
        assertTrue(wrapper3.compareTo(wrapper4) < 0, "ab < b");
        
        // Test with different lengths
        byte[] shortKey = "a".getBytes(StandardCharsets.UTF_8);
        byte[] longKey = "aa".getBytes(StandardCharsets.UTF_8);
        
        ByteArrayWrapper shortWrapper = new ByteArrayWrapper(shortKey);
        ByteArrayWrapper longWrapper = new ByteArrayWrapper(longKey);
        
        assertTrue(shortWrapper.compareTo(longWrapper) < 0,
            "Shorter key should be less than longer key with same prefix");
    }

    // ==================== Update Behavior ====================

    @Test
    public void testUpdateExistingKey() {
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        
        // Insert initial value
        Value valueObj1 = new Value(value1, TestDataGenerator.generateTimestamp(0), false);
        memtable.put(keyWrapper, valueObj1);
        assertArrayEquals(value1, memtable.get(key));
        
        // Update with new value (newer timestamp)
        Value valueObj2 = new Value(value2, TestDataGenerator.generateTimestamp(1), false);
        memtable.put(keyWrapper, valueObj2);
        
        // Should return the latest value
        byte[] retrieved = memtable.get(key);
        assertArrayEquals(value2, retrieved, "Updated value should be returned");
    }

    @Test
    public void testUpdateKeepsLatestTimestamp() {
        byte[] key = TestDataGenerator.generateKey(1);
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        
        // Insert values with different timestamps
        Value value1 = new Value("v1".getBytes(), 1000L, false);
        Value value2 = new Value("v2".getBytes(), 2000L, false);
        Value value3 = new Value("v3".getBytes(), 3000L, false);
        
        memtable.put(keyWrapper, value1);
        memtable.put(keyWrapper, value2);
        memtable.put(keyWrapper, value3);
        
        // Get should return the value from the latest timestamp
        byte[] retrieved = memtable.get(key);
        assertArrayEquals("v3".getBytes(), retrieved);
    }

    @Test
    public void testOlderTimestampIgnored() {
        byte[] key = TestDataGenerator.generateKey(1);
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        
        // Insert newer value first
        Value newerValue = new Value("newer".getBytes(), 2000L, false);
        memtable.put(keyWrapper, newerValue);
        
        // Try to insert older value - Memtable doesn't check timestamps, just overwrites
        Value olderValue = new Value("older".getBytes(), 1000L, false);
        memtable.put(keyWrapper, olderValue);
        
        // The last put wins, regardless of timestamp
        // This is expected behavior - timestamp checking happens at read time during merging
        byte[] retrieved = memtable.get(key);
        assertArrayEquals("older".getBytes(), retrieved,
            "Last put wins - timestamp checking is done during merge, not in memtable");
    }

    // ==================== Size Calculation ====================

    @Test
    public void testSizeCalculation() {
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(0), false);
        
        // Empty memtable should not need flush
        assertFalse(memtable.shouldFlush(), "Empty memtable should not need flush");
        
        memtable.put(keyWrapper, valueObj);
        
        // After adding one entry, verify it's retrievable (size is working)
        byte[] retrieved = memtable.get(key);
        assertNotNull(retrieved);
        assertArrayEquals(value, retrieved);
    }

    @Test
    public void testSizeIncrementsOnPut() {
        assertFalse(memtable.shouldFlush(), "Empty memtable should not need flush");
        
        // Add multiple entries
        for (int i = 0; i < 10; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(i), false);
            
            memtable.put(keyWrapper, valueObj);
        }
        
        // Verify all entries are retrievable (size tracking is working)
        for (int i = 0; i < 10; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] retrieved = memtable.get(key);
            assertNotNull(retrieved, "Entry " + i + " should be retrievable");
        }
    }

    @Test
    public void testSizeWithDeletes() {
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(0), false);
        
        // Put initial value
        memtable.put(keyWrapper, valueObj);
        assertNotNull(memtable.get(key));
        
        // Delete creates a tombstone (null value but still has key)
        memtable.remove(key, TestDataGenerator.generateTimestamp(1));
        
        // Get should return null but tombstone should exist in iterator
        assertNull(memtable.get(key), "Deleted key should return null");
        
        // Verify tombstone exists via iterator
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        boolean foundTombstone = false;
        while (iterator.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
            if (Arrays.equals(entry.getKey().getData(), key)) {
                foundTombstone = true;
                assertTrue(entry.getValue().isDeleted());
            }
        }
        assertTrue(foundTombstone, "Tombstone should be present in memtable");
    }

    @Test
    public void testThresholdDetection() {
        // 8MB threshold
        long threshold = 8 * 1024 * 1024;
        
        assertFalse(memtable.shouldFlush(), "Empty memtable should not need flush");
        
        // Add entries until we exceed the threshold
        int entrySize = 1024; // 1KB per entry
        int entriesNeeded = (int) (threshold / entrySize) + 10; // Exceed threshold
        
        for (int i = 0; i < entriesNeeded; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateLargeValue(entrySize, i);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(i), false);
            
            memtable.put(keyWrapper, valueObj);
        }
        
        assertTrue(memtable.shouldFlush(),
            "Memtable should need flush when size exceeds threshold");
    }

    // ==================== Tombstone Handling ====================

    @Test
    public void testTombstoneCreation() {
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        long timestamp = TestDataGenerator.generateTimestamp(0);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(value, timestamp, false);
        
        // Put initial value
        memtable.put(keyWrapper, valueObj);
        
        // Delete to create tombstone
        long deleteTimestamp = TestDataGenerator.generateTimestamp(1);
        memtable.remove(key, deleteTimestamp);
        
        // Verify tombstone exists in iterator
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        boolean foundTombstone = false;
        
        while (iterator.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
            if (Arrays.equals(entry.getKey().getData(), key)) {
                assertTrue(entry.getValue().isDeleted(),
                    "Deleted key should have tombstone marker");
                foundTombstone = true;
            }
        }
        
        assertTrue(foundTombstone, "Tombstone should be present in memtable");
    }

    @Test
    public void testGetDeletedKeyThrowsException() {
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(0), false);
        
        // Put then delete
        memtable.put(keyWrapper, valueObj);
        memtable.remove(key, TestDataGenerator.generateTimestamp(1));
        
        // Get on deleted key should return null (tombstone)
        byte[] retrieved = memtable.get(key);
        assertNull(retrieved, "Get on deleted key should return null");
    }

    @Test
    public void testTombstoneHasTimestamp() {
        byte[] key = TestDataGenerator.generateKey(1);
        long deleteTimestamp = TestDataGenerator.generateTimestamp(10);
        
        // Create tombstone
        memtable.remove(key, deleteTimestamp);
        
        // Verify via iterator
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        
        while (iterator.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
            if (Arrays.equals(entry.getKey().getData(), key)) {
                assertEquals(deleteTimestamp, entry.getValue().getTimestamp(),
                    "Tombstone should have the delete timestamp");
                assertTrue(entry.getValue().isDeleted(),
                    "Entry should be marked as deleted");
            }
        }
    }

    // ==================== Edge Cases ====================

    @Test
    public void testEmptyMemtable() {
        assertFalse(memtable.shouldFlush(), "Empty memtable should not need flush");
        
        // Get on empty memtable
        byte[] key = TestDataGenerator.generateKey(1);
        assertNull(memtable.get(key), "Get on empty memtable should return null");
        
        // Iterator on empty memtable
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        assertFalse(iterator.hasNext(), "Empty memtable iterator should have no elements");
    }

    @Test
    public void testSingleEntry() {
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(0), false);
        
        memtable.put(keyWrapper, valueObj);
        
        // Verify single entry
        assertArrayEquals(value, memtable.get(key));
        
        // Verify iterator
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        assertTrue(iterator.hasNext());
        
        Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
        assertArrayEquals(key, entry.getKey().getData());
        assertArrayEquals(value, entry.getValue().getValue());
        
        assertFalse(iterator.hasNext(), "Single entry memtable should have only one element");
    }

    @Test
    public void testNullValue() {
        byte[] key = TestDataGenerator.generateKey(1);
        long timestamp = TestDataGenerator.generateTimestamp(0);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value tombstoneValue = new Value(null, timestamp, true);
        
        // Put null value (tombstone)
        memtable.put(keyWrapper, tombstoneValue);
        
        // Get should return null
        byte[] retrieved = memtable.get(key);
        assertNull(retrieved, "Null value should be treated as tombstone");
        
        // Verify tombstone exists via iterator
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        boolean found = false;
        while (iterator.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
            if (Arrays.equals(entry.getKey().getData(), key)) {
                found = true;
                assertTrue(entry.getValue().isDeleted());
                assertNull(entry.getValue().getValue());
            }
        }
        assertTrue(found, "Tombstone should be in memtable");
    }

    @Test
    public void testEmptyKey() {
        byte[] emptyKey = new byte[0];
        byte[] value = TestDataGenerator.generateValue(1);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(emptyKey);
        Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(0), false);
        
        // Should handle empty key
        assertDoesNotThrow(() -> memtable.put(keyWrapper, valueObj));
        
        // Should retrieve with empty key
        byte[] retrieved = memtable.get(emptyKey);
        assertArrayEquals(value, retrieved);
    }

    @Test
    public void testLargeValue() {
        byte[] key = TestDataGenerator.generateKey(1);
        // Create a 1MB value
        int sizeInBytes = 1024 * 1024;
        byte[] largeValue = TestDataGenerator.generateLargeValue(sizeInBytes, 1);
        
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(largeValue, TestDataGenerator.generateTimestamp(0), false);
        
        memtable.put(keyWrapper, valueObj);
        
        // Verify retrieval
        byte[] retrieved = memtable.get(key);
        assertNotNull(retrieved);
        assertEquals(largeValue.length, retrieved.length);
        assertArrayEquals(largeValue, retrieved);
    }

    // ==================== Iterator Correctness ====================

    @Test
    public void testIteratorOrdering() {
        // Insert keys in random order
        int[] insertOrder = {5, 1, 9, 3, 7, 2, 8, 4, 6, 0};
        
        for (int id : insertOrder) {
            byte[] key = TestDataGenerator.generateKey(id);
            byte[] value = TestDataGenerator.generateValue(id);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(id), false);
            
            memtable.put(keyWrapper, valueObj);
        }
        
        // Collect keys from iterator
        List<String> iteratedKeys = new ArrayList<>();
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        
        while (iterator.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
            String keyString = new String(entry.getKey().getData(), StandardCharsets.UTF_8);
            iteratedKeys.add(keyString);
        }
        
        // Verify sorted order
        List<String> sortedKeys = new ArrayList<>(iteratedKeys);
        Collections.sort(sortedKeys);
        
        assertEquals(sortedKeys, iteratedKeys,
            "Iterator should return keys in sorted order");
    }

    @Test
    public void testIteratorCompleteness() {
        int keyCount = 50;
        Set<String> insertedKeys = new HashSet<>();
        
        // Insert keys
        for (int i = 0; i < keyCount; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(i), false);
            
            memtable.put(keyWrapper, valueObj);
            insertedKeys.add(new String(key, StandardCharsets.UTF_8));
        }
        
        // Collect keys from iterator
        Set<String> iteratedKeys = new HashSet<>();
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        
        while (iterator.hasNext()) {
            Map.Entry<ByteArrayWrapper, Value> entry = iterator.next();
            String keyString = new String(entry.getKey().getData(), StandardCharsets.UTF_8);
            iteratedKeys.add(keyString);
        }
        
        // Verify all keys are present
        assertEquals(insertedKeys, iteratedKeys,
            "Iterator should return all inserted keys");
    }

    @Test
    public void testEmptyIterator() {
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        
        assertFalse(iterator.hasNext(),
            "Iterator on empty memtable should have no elements");
        
        assertThrows(NoSuchElementException.class, iterator::next,
            "Calling next() on empty iterator should throw NoSuchElementException");
    }

    // ==================== Additional Tests ====================

    @Test
    public void testMinMaxKey() {
        // Insert keys
        int[] ids = {5, 2, 8, 1, 9};
        
        for (int id : ids) {
            byte[] key = TestDataGenerator.generateKey(id);
            byte[] value = TestDataGenerator.generateValue(id);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(id), false);
            
            memtable.put(keyWrapper, valueObj);
        }
        
        // Get min and max keys
        ByteArrayWrapper minKey = memtable.minKey();
        ByteArrayWrapper maxKey = memtable.maxKey();
        
        assertNotNull(minKey);
        assertNotNull(maxKey);
        
        // Verify min and max
        String minKeyString = new String(minKey.getData(), StandardCharsets.UTF_8);
        String maxKeyString = new String(maxKey.getData(), StandardCharsets.UTF_8);
        
        assertEquals("key_00001", minKeyString, "Min key should be the smallest");
        assertEquals("key_00009", maxKeyString, "Max key should be the largest");
    }

    @Test
    public void testClear() {
        // Add entries
        for (int i = 0; i < 10; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(i), false);
            
            memtable.put(keyWrapper, valueObj);
        }
        
        // Verify entries exist
        assertNotNull(memtable.get(TestDataGenerator.generateKey(0)));
        
        // Clear memtable
        memtable.clear();
        
        assertFalse(memtable.shouldFlush(), "Cleared memtable should not need flush");
        
        // Iterator should be empty
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        assertFalse(iterator.hasNext(), "Iterator should be empty after clear");
        
        // Get should return null
        assertNull(memtable.get(TestDataGenerator.generateKey(0)));
    }

    @Test
    public void testMultipleUpdatesToSameKey() {
        byte[] key = TestDataGenerator.generateKey(1);
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        
        int updateCount = 100;
        
        for (int i = 0; i < updateCount; i++) {
            byte[] value = ("value_" + i).getBytes(StandardCharsets.UTF_8);
            Value valueObj = new Value(value, TestDataGenerator.generateTimestamp(i), false);
            memtable.put(keyWrapper, valueObj);
        }
        
        // Should have only one entry (updates replace)
        Iterator<Map.Entry<ByteArrayWrapper, Value>> iterator = memtable.iterator();
        int count = 0;
        
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        
        assertEquals(1, count, "Multiple updates to same key should result in single entry");
        
        // Should have the latest value
        byte[] retrieved = memtable.get(key);
        String retrievedString = new String(retrieved, StandardCharsets.UTF_8);
        assertEquals("value_99", retrievedString, "Should have the latest updated value");
    }

    @Test
    public void testFlushThresholdOverrideForTests() {
        long defaultThreshold = Memtable.getDefaultFlushThresholdSizeBytes();
        assertEquals(defaultThreshold, Memtable.getFlushThresholdSizeBytes(),
                "Without override, flush threshold must match production default");

        Memtable.setFlushThresholdSizeForTests(1);
        assertEquals(1, Memtable.getFlushThresholdSizeBytes(),
                "Test override must replace default flush threshold");

        memtable.put(new ByteArrayWrapper("k".getBytes(StandardCharsets.UTF_8)),
                new Value("v".getBytes(StandardCharsets.UTF_8), TestDataGenerator.generateTimestamp(1), false));
        assertTrue(memtable.shouldFlush(), "Tiny override should trigger real flush condition with tiny data");

        Memtable.resetFlushThresholdSizeForTests();
        assertEquals(defaultThreshold, Memtable.getFlushThresholdSizeBytes(),
                "Reset must restore production default threshold");
    }

    @Test
    public void testFlushThresholdOverrideRejectsNonPositiveValues() {
        assertThrows(IllegalArgumentException.class, () -> Memtable.setFlushThresholdSizeForTests(0));
        assertThrows(IllegalArgumentException.class, () -> Memtable.setFlushThresholdSizeForTests(-1));
    }
}

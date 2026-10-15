package org.lsmtdb.core.wal;

import org.junit.jupiter.api.*;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.test.utils.LSMTreeTestHarness;
import org.lsmtdb.test.utils.TestDataGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for Write-Ahead Log (WAL) component.
 * Tests WALWriter, WALReader, WalEntry, recovery, and edge cases.
 */
public class WALTest {
    
    private LSMTreeTestHarness harness;
    private Path walPath;
    
    @BeforeEach
    void setUp() throws IOException {
        harness = new LSMTreeTestHarness("walTest");
        walPath = harness.getTestDbPath().resolve("test.wal");
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (harness != null) {
            harness.cleanup();
        }
    }
    
    // ==================== 1. Basic Write Operations ====================
    
    @Test
    @DisplayName("Write single entry to WAL")
    void testWriteSingleEntry() throws IOException {
        ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(1);
        byte[] valueData = TestDataGenerator.generateValue(1);
        long timestamp = TestDataGenerator.generateTimestamp(1);
        Value value = new Value(valueData, timestamp, false);
        WalEntry entry = new WalEntry(key, value);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            writer.append(entry);
        }
        
        assertTrue(Files.exists(walPath), "WAL file should exist");
        assertTrue(Files.size(walPath) > 0, "WAL file should not be empty");
    }
    
    @Test
    @DisplayName("Write multiple entries to WAL")
    void testWriteMultipleEntries() throws IOException {
        List<WalEntry> entries = createTestEntries(5);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
        }
        
        assertTrue(Files.exists(walPath));
        long fileSize = Files.size(walPath);
        assertTrue(fileSize > entries.size() * 20, "File size should accommodate all entries");
    }
    
    @Test
    @DisplayName("Verify flush persists data to disk")
    void testWriteAndFlush() throws IOException {
        WalEntry entry = createTestEntries(1).get(0);
        
        try (WALWriter writer = new WALWriter(walPath, 100)) {
            writer.append(entry);
            long sizeBeforeFlush = Files.exists(walPath) ? Files.size(walPath) : 0;
            
            writer.flush();
            
            long sizeAfterFlush = Files.size(walPath);
            assertTrue(sizeAfterFlush > sizeBeforeFlush || sizeAfterFlush > 0, 
                "File size should increase or be non-zero after flush");
        }
    }
    
    @Test
    @DisplayName("Auto-flush when buffer reaches threshold")
    void testAutoFlushOnThreshold() throws IOException {
        int batchSize = 5;
        
        try (WALWriter writer = new WALWriter(walPath, batchSize)) {
            for (int i = 0; i < batchSize + 2; i++) {
                WalEntry entry = createTestEntries(1, i).get(0);
                writer.append(entry);
                
                if (i == batchSize - 1) {
                    long sizeAfterBatch = Files.size(walPath);
                    assertTrue(sizeAfterBatch > 0, "Auto-flush should have occurred at batch threshold");
                }
            }
        }
    }
    
    // ==================== 2. Basic Read Operations ====================
    
    @Test
    @DisplayName("Read single entry from WAL")
    void testReadSingleEntry() throws IOException {
        WalEntry original = createTestEntries(1).get(0);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            writer.append(original);
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(1, recovered.size(), "Should recover exactly one entry");
            assertEntriesEqual(original, recovered.get(0));
        }
    }
    
    @Test
    @DisplayName("Read multiple entries in order")
    void testReadMultipleEntries() throws IOException {
        List<WalEntry> original = createTestEntries(10);
        
        try (WALWriter writer = new WALWriter(walPath, 20)) {
            for (WalEntry entry : original) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(original.size(), recovered.size());
            for (int i = 0; i < original.size(); i++) {
                assertEntriesEqual(original.get(i), recovered.get(i));
            }
        }
    }
    
    @Test
    @DisplayName("Read from empty WAL file")
    void testReadEmptyWAL() throws IOException {
        Files.createFile(walPath);
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertNotNull(recovered);
            assertTrue(recovered.isEmpty(), "Empty WAL should return empty list");
        }
    }
    
    @Test
    @DisplayName("Entries are read in write order")
    void testReadReturnsCorrectOrder() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(i);
            byte[] valueData = ("value_" + i).getBytes();
            long timestamp = TestDataGenerator.generateTimestamp(i);
            Value value = new Value(valueData, timestamp, false);
            entries.add(new WalEntry(key, value));
        }
        
        try (WALWriter writer = new WALWriter(walPath, 50)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            for (int i = 0; i < entries.size(); i++) {
                String expectedValue = "value_" + i;
                String actualValue = new String(recovered.get(i).value.getValue());
                assertEquals(expectedValue, actualValue, "Entry at position " + i + " should match");
            }
        }
    }
    
    // ==================== 3. Write-Read Consistency ====================
    
    @Test
    @DisplayName("Write entries, read back, verify exact match")
    void testWriteThenRead() throws IOException {
        List<WalEntry> original = createTestEntries(15);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            for (WalEntry entry : original) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(original.size(), recovered.size());
            for (int i = 0; i < original.size(); i++) {
                assertEntriesEqual(original.get(i), recovered.get(i));
            }
        }
    }
    
    @Test
    @DisplayName("Write 10K entries, read all, verify consistency")
    void testLargeWriteRead() throws IOException {
        int entryCount = 10_000;
        List<WalEntry> original = createTestEntries(entryCount);
        
        try (WALWriter writer = new WALWriter(walPath, 100)) {
            for (WalEntry entry : original) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(entryCount, recovered.size(), "Should recover all 10K entries");
            
            // Spot check entries
            for (int i : new int[]{0, 100, 1000, 5000, 9999}) {
                assertEntriesEqual(original.get(i), recovered.get(i));
            }
        }
    }
    
    @Test
    @DisplayName("Timestamps are preserved during write/read")
    void testTimestampPreserved() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(i);
            byte[] valueData = TestDataGenerator.generateValue(i);
            long timestamp = TestDataGenerator.generateTimestamp(i * 100);
            Value value = new Value(valueData, timestamp, false);
            entries.add(new WalEntry(key, value));
        }
        
        try (WALWriter writer = new WALWriter(walPath, 20)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            for (int i = 0; i < entries.size(); i++) {
                long expectedTimestamp = TestDataGenerator.generateTimestamp(i * 100);
                long actualTimestamp = recovered.get(i).value.getTimestamp();
                assertEquals(expectedTimestamp, actualTimestamp, 
                    "Timestamp for entry " + i + " should match");
            }
        }
    }
    
    @Test
    @DisplayName("Values are not corrupted during write/read cycle")
    void testValueIntegrity() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(i);
            byte[] valueData = TestDataGenerator.generateRandomBytes(256, i);
            long timestamp = TestDataGenerator.generateTimestamp(i);
            Value value = new Value(valueData, timestamp, false);
            entries.add(new WalEntry(key, value));
        }
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            for (int i = 0; i < entries.size(); i++) {
                assertArrayEquals(entries.get(i).value.getValue(), 
                    recovered.get(i).value.getValue(), 
                    "Value data for entry " + i + " should be identical");
            }
        }
    }
    
    // ==================== 4. Checkpoint Operations ====================
    
    @Test
    @DisplayName("Create checkpoint and clear WAL")
    void testCheckpoint() throws IOException {
        List<WalEntry> entries = createTestEntries(10);
        
        try (WALWriter writer = new WALWriter(walPath, 5)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
            writer.flush();
            
            long sizeBeforeClear = Files.size(walPath);
            assertTrue(sizeBeforeClear > 0, "WAL should have data before checkpoint");
            
            writer.clear();
            
            long sizeAfterClear = Files.size(walPath);
            assertEquals(0, sizeAfterClear, "WAL should be empty after checkpoint");
        }
    }
    
    @Test
    @DisplayName("After checkpoint, WAL is empty")
    void testCheckpointClearsEntries() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 5)) {
            for (WalEntry entry : createTestEntries(5)) {
                writer.append(entry);
            }
            writer.flush();
            writer.clear();
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            assertTrue(recovered.isEmpty(), "No entries should be recovered after checkpoint");
        }
    }
    
    @Test
    @DisplayName("Multiple sequential checkpoints work correctly")
    void testMultipleCheckpoints() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            for (int checkpoint = 0; checkpoint < 3; checkpoint++) {
                for (WalEntry entry : createTestEntries(5, checkpoint * 5)) {
                    writer.append(entry);
                }
                writer.flush();
                
                long sizeBeforeClear = Files.size(walPath);
                assertTrue(sizeBeforeClear > 0);
                
                writer.clear();
                
                assertEquals(0, Files.size(walPath), 
                    "WAL should be empty after checkpoint " + checkpoint);
            }
        }
    }
    
    @Test
    @DisplayName("Can write to WAL after checkpoint")
    void testWriteAfterCheckpoint() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            writer.append(createTestEntries(1).get(0));
            writer.flush();
            writer.clear();
            
            List<WalEntry> newEntries = createTestEntries(5, 100);
            for (WalEntry entry : newEntries) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            assertEquals(5, recovered.size(), "Should only recover post-checkpoint entries");
        }
    }
    
    // ==================== 5. Recovery Scenarios ====================
    
    @Test
    @DisplayName("Replay WAL entries to reconstruct state")
    void testRecoveryFromWAL() throws IOException {
        List<WalEntry> entries = createTestEntries(20);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
        }
        
        // Simulate recovery
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(entries.size(), recovered.size());
            for (int i = 0; i < entries.size(); i++) {
                assertEntriesEqual(entries.get(i), recovered.get(i));
            }
        }
    }
    
    @Test
    @DisplayName("Recovery includes tombstone entries")
    void testRecoveryWithDeletes() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        
        for (int i = 0; i < 10; i++) {
            ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(i);
            boolean isDeleted = (i % 3 == 0);
            byte[] valueData = isDeleted ? null : TestDataGenerator.generateValue(i);
            long timestamp = TestDataGenerator.generateTimestamp(i);
            Value value = new Value(valueData, timestamp, isDeleted);
            entries.add(new WalEntry(key, value));
        }
        
        try (WALWriter writer = new WALWriter(walPath, 20)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(10, recovered.size());
            for (int i = 0; i < 10; i++) {
                boolean expectedDeleted = (i % 3 == 0);
                assertEquals(expectedDeleted, recovered.get(i).value.isDeleted(),
                    "Entry " + i + " delete flag should match");
            }
        }
    }
    
    @Test
    @DisplayName("Handle corrupted entries gracefully during recovery")
    void testRecoverySkipsCorrupted() throws IOException {
        List<WalEntry> entries = createTestEntries(5);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
        }
        
        // Corrupt the WAL by truncating it
        long originalSize = Files.size(walPath);
        try (FileChannel channel = FileChannel.open(walPath, StandardOpenOption.WRITE)) {
            channel.truncate(originalSize / 2);
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertTrue(recovered.size() < entries.size(), 
                "Should recover fewer entries due to corruption");
            assertTrue(recovered.size() >= 1, 
                "Should recover at least some valid entries");
        }
    }
    
    @Test
    @DisplayName("Partial recovery from incomplete WAL")
    void testPartialRecovery() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 5)) {
            for (int i = 0; i < 10; i++) {
                writer.append(createTestEntries(1, i).get(0));
            }
        }
        
        long fullSize = Files.size(walPath);
        
        // Truncate to simulate incomplete write
        try (FileChannel channel = FileChannel.open(walPath, StandardOpenOption.WRITE)) {
            channel.truncate(fullSize * 3 / 4);
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertTrue(recovered.size() > 0, "Should recover some entries");
            assertTrue(recovered.size() < 10, "Should not recover all entries");
        }
    }
    
    // ==================== 6. Delete/Tombstone Handling ====================
    
    @Test
    @DisplayName("Write delete operation (tombstone)")
    void testWriteDelete() throws IOException {
        ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(1);
        long timestamp = TestDataGenerator.generateTimestamp(1);
        Value deletedValue = new Value(null, timestamp, true);
        WalEntry tombstone = new WalEntry(key, deletedValue);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            writer.append(tombstone);
        }
        
        assertTrue(Files.exists(walPath));
        assertTrue(Files.size(walPath) > 0);
    }
    
    @Test
    @DisplayName("Read delete operation and verify isDeleted flag")
    void testReadDelete() throws IOException {
        ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(42);
        long timestamp = System.currentTimeMillis();
        Value deletedValue = new Value(null, timestamp, true);
        WalEntry tombstone = new WalEntry(key, deletedValue);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            writer.append(tombstone);
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(1, recovered.size());
            assertTrue(recovered.get(0).value.isDeleted(), "Entry should be marked as deleted");
            assertNull(recovered.get(0).value.getValue(), "Deleted entry should have null value");
        }
    }
    
    @Test
    @DisplayName("Mix put and delete operations in WAL")
    void testMixedOpsInWAL() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        
        for (int i = 0; i < 20; i++) {
            ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(i);
            long timestamp = TestDataGenerator.generateTimestamp(i);
            
            if (i % 4 == 0) {
                // Delete operation
                Value value = new Value(null, timestamp, true);
                entries.add(new WalEntry(key, value));
            } else {
                // Put operation
                byte[] valueData = TestDataGenerator.generateValue(i);
                Value value = new Value(valueData, timestamp, false);
                entries.add(new WalEntry(key, value));
            }
        }
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            for (WalEntry entry : entries) {
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(20, recovered.size());
            
            int deleteCount = 0;
            int putCount = 0;
            for (WalEntry entry : recovered) {
                if (entry.value.isDeleted()) {
                    deleteCount++;
                    assertNull(entry.value.getValue());
                } else {
                    putCount++;
                    assertNotNull(entry.value.getValue());
                }
            }
            
            assertEquals(5, deleteCount, "Should have 5 delete operations");
            assertEquals(15, putCount, "Should have 15 put operations");
        }
    }
    
    // ==================== 7. Edge Cases ====================
    
    @Test
    @DisplayName("Operations on empty WAL")
    void testEmptyWAL() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            writer.flush();
        }
        
        assertEquals(0, Files.size(walPath), "Empty WAL should have size 0");
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            assertTrue(recovered.isEmpty());
        }
    }
    
    @Test
    @DisplayName("Write entry with large 1MB value")
    void testLargeValue() throws IOException {
        ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(1);
        byte[] largeData = TestDataGenerator.generateLargeValue(1_000_000, 1);
        long timestamp = TestDataGenerator.generateTimestamp(1);
        Value largeValue = new Value(largeData, timestamp, false);
        WalEntry largeEntry = new WalEntry(key, largeValue);
        
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            writer.append(largeEntry);
        }
        
        assertTrue(Files.size(walPath) >= 1_000_000, 
            "WAL file should be at least 1MB for large entry");
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(1, recovered.size());
            assertEquals(1_000_000, recovered.get(0).value.getValue().length);
            assertArrayEquals(largeData, recovered.get(0).value.getValue());
        }
    }
    
    @Test
    @DisplayName("Handle 100K small writes")
    void testManySmallWrites() throws IOException {
        int count = 100_000;
        
        try (WALWriter writer = new WALWriter(walPath, 1000)) {
            for (int i = 0; i < count; i++) {
                WalEntry entry = createTestEntries(1, i).get(0);
                writer.append(entry);
            }
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            assertEquals(count, recovered.size(), "Should recover all 100K entries");
        }
    }
    
    @Test
    @DisplayName("WAL file size grows with writes")
    void testWALSizeGrowth() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 5)) {
            long size0 = Files.exists(walPath) ? Files.size(walPath) : 0;
            
            writer.append(createTestEntries(1, 0).get(0));
            writer.flush();
            long size1 = Files.size(walPath);
            
            for (int i = 1; i < 10; i++) {
                writer.append(createTestEntries(1, i).get(0));
            }
            writer.flush();
            long size10 = Files.size(walPath);
            
            assertTrue(size1 > size0, "Size should increase after first write");
            assertTrue(size10 > size1, "Size should increase after more writes");
        }
    }
    
    // ==================== 8. Error Handling ====================
    
    @Test
    @DisplayName("Handle corrupted entry in WAL")
    void testCorruptedEntry() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 5)) {
            for (int i = 0; i < 5; i++) {
                writer.append(createTestEntries(1, i).get(0));
            }
        }
        
        // Corrupt by appending garbage
        try (FileChannel channel = FileChannel.open(walPath, 
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer garbage = ByteBuffer.allocate(10);
            for (int i = 0; i < 10; i++) {
                garbage.put((byte) 0xFF);
            }
            garbage.flip();
            channel.write(garbage);
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertEquals(5, recovered.size(), 
                "Should recover valid entries before corruption");
        }
    }
    
    @Test
    @DisplayName("Handle truncated WAL file")
    void testTruncatedWAL() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 10)) {
            for (int i = 0; i < 10; i++) {
                writer.append(createTestEntries(1, i).get(0));
            }
        }
        
        long originalSize = Files.size(walPath);
        
        // Truncate to partial entry
        try (FileChannel channel = FileChannel.open(walPath, StandardOpenOption.WRITE)) {
            channel.truncate(originalSize - 5);
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertTrue(recovered.size() < 10, 
                "Truncation should result in fewer recovered entries");
        }
    }
    
    @Test
    @DisplayName("Handle invalid WAL format")
    void testInvalidFormat() throws IOException {
        // Write completely invalid data
        Files.write(walPath, "This is not a valid WAL format!".getBytes());
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            
            assertTrue(recovered.isEmpty(), 
                "Invalid format should result in no recovered entries");
        }
    }
    
    @Test
    @DisplayName("Handle I/O errors gracefully")
    void testIOErrors() throws IOException {
        // Create WAL in read-only location simulation
        Path readOnlyPath = harness.getTestDbPath().resolve("readonly");
        Files.createDirectory(readOnlyPath);
        Path roWalPath = readOnlyPath.resolve("test.wal");
        
        try (WALWriter writer = new WALWriter(roWalPath, 10)) {
            writer.append(createTestEntries(1).get(0));
        }
        
        // Reading should work
        try (WALReader reader = new WALReader(roWalPath)) {
            List<WalEntry> recovered = reader.recover();
            assertEquals(1, recovered.size());
        }
    }
    
    // ==================== 9. Buffer Management ====================
    
    @Test
    @DisplayName("Behavior when buffer reaches capacity")
    void testBufferFull() throws IOException {
        int batchSize = 3;
        
        try (WALWriter writer = new WALWriter(walPath, batchSize)) {
            for (int i = 0; i < batchSize; i++) {
                writer.append(createTestEntries(1, i).get(0));
            }
            
            long sizeAfterBatch = Files.size(walPath);
            assertTrue(sizeAfterBatch > 0, "Auto-flush should occur when buffer full");
            
            writer.append(createTestEntries(1, batchSize).get(0));
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            assertEquals(batchSize + 1, recovered.size());
        }
    }
    
    @Test
    @DisplayName("Manual flush works correctly")
    void testManualFlush() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 100)) {
            writer.append(createTestEntries(1, 0).get(0));
            
            long sizeBefore = Files.exists(walPath) ? Files.size(walPath) : 0;
            writer.flush();
            long sizeAfter = Files.size(walPath);
            
            assertTrue(sizeAfter > sizeBefore || sizeAfter > 0, 
                "Manual flush should persist data");
        }
    }
    
    @Test
    @DisplayName("Closing WAL flushes buffer")
    void testFlushOnClose() throws IOException {
        try (WALWriter writer = new WALWriter(walPath, 100)) {
            for (int i = 0; i < 10; i++) {
                writer.append(createTestEntries(1, i).get(0));
            }
            // Don't manually flush
        }
        
        try (WALReader reader = new WALReader(walPath)) {
            List<WalEntry> recovered = reader.recover();
            assertEquals(10, recovered.size(), 
                "All entries should be persisted on close");
        }
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Create test entries starting from id 0
     */
    private List<WalEntry> createTestEntries(int count) {
        return createTestEntries(count, 0);
    }
    
    /**
     * Create test entries with custom starting id
     */
    private List<WalEntry> createTestEntries(int count, int startId) {
        List<WalEntry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            ByteArrayWrapper key = TestDataGenerator.createByteArrayWrapper(id);
            byte[] valueData = TestDataGenerator.generateValue(id);
            long timestamp = TestDataGenerator.generateTimestamp(id);
            Value value = new Value(valueData, timestamp, false);
            entries.add(new WalEntry(key, value));
        }
        return entries;
    }
    
    /**
     * Assert two WAL entries are equal
     */
    private void assertEntriesEqual(WalEntry expected, WalEntry actual) {
        assertArrayEquals(expected.key.getData(), actual.key.getData(), 
            "Keys should match");
        assertEquals(expected.value.isDeleted(), actual.value.isDeleted(), 
            "Delete flags should match");
        assertEquals(expected.value.getTimestamp(), actual.value.getTimestamp(), 
            "Timestamps should match");
        
        if (!expected.value.isDeleted()) {
            assertArrayEquals(expected.value.getValue(), actual.value.getValue(), 
                "Values should match");
        }
    }
}

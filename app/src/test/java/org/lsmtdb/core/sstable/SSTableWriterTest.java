package org.lsmtdb.core.sstable;

import org.junit.jupiter.api.*;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.Memtable;
import org.lsmtdb.test.utils.SSTableInspector;
import org.lsmtdb.test.utils.TestDataGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for SSTableWriter component.
 * Tests cover basic writing, key ordering, indexing, metadata, tombstones, timestamps, and edge cases.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SSTableWriterTest {

    private Path testDir;
    private TableDirectory tableDirectory;

    @BeforeAll
    public void setupAll() throws IOException {
        // Create isolated test directory
        testDir = Paths.get("test_sstables_" + System.currentTimeMillis());
        Files.createDirectories(testDir);
        
        // Initialize TableDirectory with test path
        org.lsmtdb.common.AppConstants.setBaseDirForTests(testDir.toString());
        TableDirectory.resetForTests();
    }

    @BeforeEach
    public void setup() throws IOException {
        // Get fresh TableDirectory instance for each test
        tableDirectory = TableDirectory.getInstance();
        tableDirectory.getAllLevels().forEach(level ->
                tableDirectory.removeSSTables(level.getLevelNumber(), new java.util.ArrayList<>(level.getSstables())));
        
        // Clear memtable for each test
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
    }

    @AfterEach
    public void cleanup() throws IOException {
        // Clear memtable after each test for isolation
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
        
        // Clean up any SSTable files created during tests
        if (Files.exists(testDir)) {
            cleanDirectory(testDir);
        }
    }

    @AfterAll
    public void teardownAll() throws IOException {
        org.lsmtdb.common.AppConstants.resetBaseDirForTests();
        // Remove test directory
        if (Files.exists(testDir)) {
            deleteDirectory(testDir.toFile());
        }
    }

    // ============================================================
    // 1. BASIC WRITING TESTS
    // ============================================================

    @Test
    public void testWriteSingleEntry() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add single entry
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        long timestamp = TestDataGenerator.generateTimestamp(0);
        
        memtable.put(new ByteArrayWrapper(key), new Value(value, timestamp, false));
        
        // Write to SSTable
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify file was created
        SSTableMetadata metadata = getLastSSTable(0);
        System.out.println("DEBUG: Reading from file: " + metadata.getFilePath());
        File sstableFile = new File(metadata.getFilePath());
        assertTrue(sstableFile.exists(), "SSTable file should exist");
        assertTrue(sstableFile.length() > 0, "SSTable file should not be empty");
        
        // Verify content
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(metadata.getFilePath());
        System.out.println("DEBUG: Read " + entries.size() + " entries from SSTable");
        assertEquals(1, entries.size(), "Should have one entry");
        assertEquals("key_00001", entries.get(0).getKeyAsString());
        assertEquals("value_00001", entries.get(0).getValueAsString());
        assertEquals(timestamp, entries.get(0).timestamp);
        assertFalse(entries.get(0).isDeleted);
    }

    @Test
    public void testWriteMultipleEntries() throws IOException {
        Memtable memtable = Memtable.getInstance();
        int entryCount = 100;
        
        // Add multiple entries
        for (int i = 0; i < entryCount; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            long timestamp = TestDataGenerator.generateTimestamp(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, timestamp, false));
        }
        
        // Write to SSTable
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify all entries written
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        assertEquals(entryCount, entries.size(), "Should have all entries");
        
        // Verify entries are correct
        for (int i = 0; i < entryCount; i++) {
            assertEquals("key_" + String.format("%05d", i), entries.get(i).getKeyAsString());
            assertEquals("value_" + String.format("%05d", i), entries.get(i).getValueAsString());
        }
    }

    @Test
    public void testWriteEmptySSTable() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Don't add any entries
        
        // Writing empty memtable should handle gracefully
        // Since SSTableWriter expects minKey/maxKey from memtable,
        // this should throw or handle empty case
        SSTableWriter writer = new SSTableWriter(0);
        
        // This might throw NullPointerException - that's expected behavior for empty memtable
        assertThrows(Exception.class, () -> {
            writer.write(memtable);
        }, "Writing empty memtable should fail or throw exception");
    }

    @Test
    public void testWriteAndClose() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add entry
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        
        // Write and close
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        writer.close();
        
        // Verify file is accessible even after close
        File file = new File(getLastSSTable(0).getFilePath());
        assertTrue(file.exists());
        
        // Verify can still read
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        assertEquals(1, entries.size());
    }

    // ============================================================
    // 2. KEY ORDERING ENFORCEMENT TESTS
    // ============================================================

    @Test
    public void testWriteSortedKeys() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Memtable automatically sorts, so add in any order
        for (int i = 0; i < 10; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        // Write to SSTable
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify keys are sorted
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        assertTrue(SSTableInspector.verifyKeyOrdering(getLastSSTable(0).getFilePath()), 
                   "Keys should be in sorted order");
    }

    @Test
    public void testWriteAscendingKeys() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add keys in ascending order
        for (int i = 0; i < 20; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify ordering
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<byte[]> keys = SSTableInspector.readAllKeys(getLastSSTable(0).getFilePath());
        
        // Check each pair is in ascending order
        for (int i = 0; i < keys.size() - 1; i++) {
            ByteArrayWrapper current = new ByteArrayWrapper(keys.get(i));
            ByteArrayWrapper next = new ByteArrayWrapper(keys.get(i + 1));
            assertTrue(current.compareTo(next) < 0, "Keys should be in ascending order");
        }
    }

    @Test
    public void testWriteUnsortedKeysFails() throws IOException {
        // Note: Memtable uses TreeMap which automatically sorts keys
        // So we can't actually test unsorted writes via Memtable
        // This test verifies that Memtable enforces sorting
        
        Memtable memtable = Memtable.getInstance();
        
        // Add keys in descending order
        for (int i = 10; i > 0; i--) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        // Write should succeed because Memtable sorts
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // But verify output is sorted
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        assertTrue(SSTableInspector.verifyKeyOrdering(getLastSSTable(0).getFilePath()));
    }

    @Test
    public void testWriteDescendingKeysFails() throws IOException {
        // Similar to above - Memtable enforces sorting
        Memtable memtable = Memtable.getInstance();
        
        // Try to add in descending order
        for (int i = 50; i >= 0; i--) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify output is in ascending order despite descending input
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<byte[]> keys = SSTableInspector.readAllKeys(getLastSSTable(0).getFilePath());
        
        for (int i = 0; i < keys.size() - 1; i++) {
            ByteArrayWrapper current = new ByteArrayWrapper(keys.get(i));
            ByteArrayWrapper next = new ByteArrayWrapper(keys.get(i + 1));
            assertTrue(current.compareTo(next) < 0);
        }
    }

    // ============================================================
    // 3. INDEX GENERATION TESTS
    // ============================================================

    @Test
    public void testIndexCreation() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add enough entries to create index (INDEX_ENTRY_INTERVAL = 128 in SSTableWriter)
        for (int i = 0; i < 300; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify SSTable was created and can be read (index is used for reading)
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        String filePath = getLastSSTable(0).getFilePath();
        
        // If we can read the SSTable, index was created successfully
        try (SSTableReader reader = new SSTableReader(filePath)) {
            assertNotNull(reader);
        }
    }

    @Test
    public void testIndexSpacing() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // INDEX_ENTRY_INTERVAL is 128, so index should be created at entries: 1, 128, 256, ...
        int entryCount = 500;
        for (int i = 0; i < entryCount; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify index works by reading all entries
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        assertEquals(entryCount, entries.size(), "All entries should be readable via index");
    }

    @Test
    public void testIndexAccuracy() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add entries
        for (int i = 0; i < 200; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify we can search for any key (index must be accurate)
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        String filePath = getLastSSTable(0).getFilePath();
        
        // Check various keys exist
        assertTrue(SSTableInspector.hasKey(filePath, TestDataGenerator.generateKey(0)));
        assertTrue(SSTableInspector.hasKey(filePath, TestDataGenerator.generateKey(50)));
        assertTrue(SSTableInspector.hasKey(filePath, TestDataGenerator.generateKey(128)));
        assertTrue(SSTableInspector.hasKey(filePath, TestDataGenerator.generateKey(199)));
    }

    @Test
    public void testFirstAndLastInIndex() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add entries - first entry (0) should always be in index
        for (int i = 0; i < 150; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        String filePath = getLastSSTable(0).getFilePath();
        
        // Verify first and last keys are accessible (must be in index or findable via index)
        assertTrue(SSTableInspector.hasKey(filePath, TestDataGenerator.generateKey(0)));
        assertTrue(SSTableInspector.hasKey(filePath, TestDataGenerator.generateKey(149)));
    }

    // ============================================================
    // 4. FOOTER METADATA TESTS
    // ============================================================

    @Test
    public void testFooterContainsMetadata() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        for (int i = 0; i < 50; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        // Verify metadata was created
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        SSTableMetadata metadata = getLastSSTable(0);
        
        assertNotNull(metadata);
        assertNotNull(metadata.getMinKey());
        assertNotNull(metadata.getMaxKey());
        assertTrue(metadata.getFileSize() > 0);
    }

    @Test
    public void testFooterOffset() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        for (int i = 0; i < 10; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        String filePath = getLastSSTable(0).getFilePath();
        
        // Verify file structure is valid (footer at correct offset)
        assertTrue(SSTableInspector.validateMetadata(filePath));
    }

    @Test
    public void testMetadataAccuracy() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        int entryCount = 75;
        for (int i = 0; i < entryCount; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        String filePath = getLastSSTable(0).getFilePath();
        
        // Verify actual content matches metadata
        int actualCount = SSTableInspector.getKeyCount(filePath);
        assertEquals(entryCount, actualCount, "Metadata count should match actual count");
    }

    @Test
    public void testMinMaxKeys() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add keys from 10 to 99
        for (int i = 10; i < 100; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        SSTableMetadata metadata = getLastSSTable(0);
        
        // Verify min/max keys
        assertEquals("key_00010", new String(metadata.getMinKey().getData(), StandardCharsets.UTF_8));
        assertEquals("key_00099", new String(metadata.getMaxKey().getData(), StandardCharsets.UTF_8));
        
        // Also verify via inspector
        byte[][] minMax = SSTableInspector.getMinMaxKeys(metadata.getFilePath());
        assertEquals("key_00010", new String(minMax[0], StandardCharsets.UTF_8));
        assertEquals("key_00099", new String(minMax[1], StandardCharsets.UTF_8));
    }

    // ============================================================
    // 5. TOMBSTONE WRITING TESTS
    // ============================================================

    @Test
    public void testWriteTombstone() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add a deleted entry (tombstone)
        byte[] key = TestDataGenerator.generateKey(1);
        long timestamp = System.currentTimeMillis();
        memtable.put(new ByteArrayWrapper(key), new Value(null, timestamp, true));
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        assertEquals(1, entries.size());
        assertTrue(entries.get(0).isDeleted, "Entry should be marked as deleted");
        assertNull(entries.get(0).value, "Deleted entry should have null value");
    }

    @Test
    public void testMixedEntriesAndTombstones() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add mix of regular and deleted entries
        for (int i = 0; i < 20; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            long timestamp = System.currentTimeMillis() + i;
            
            if (i % 3 == 0) {
                // Every 3rd entry is a tombstone
                memtable.put(new ByteArrayWrapper(key), new Value(null, timestamp, true));
            } else {
                byte[] value = TestDataGenerator.generateValue(i);
                memtable.put(new ByteArrayWrapper(key), new Value(value, timestamp, false));
            }
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        assertEquals(20, entries.size());
        
        // Verify tombstones
        int tombstoneCount = 0;
        for (int i = 0; i < entries.size(); i++) {
            if (i % 3 == 0) {
                assertTrue(entries.get(i).isDeleted);
                tombstoneCount++;
            } else {
                assertFalse(entries.get(i).isDeleted);
            }
        }
        
        assertTrue(tombstoneCount > 0, "Should have tombstones");
    }

    @Test
    public void testAllTombstones() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add only tombstones
        for (int i = 0; i < 10; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            memtable.put(new ByteArrayWrapper(key), new Value(null, System.currentTimeMillis(), true));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        assertEquals(10, entries.size());
        
        // All should be tombstones
        for (SSTableInspector.SSTableEntry entry : entries) {
            assertTrue(entry.isDeleted);
        }
    }

    // ============================================================
    // 6. TIMESTAMP HANDLING TESTS
    // ============================================================

    @Test
    public void testTimestampWritten() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        long expectedTimestamp = TestDataGenerator.generateTimestamp(100);
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] value = TestDataGenerator.generateValue(1);
        
        memtable.put(new ByteArrayWrapper(key), new Value(value, expectedTimestamp, false));
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        assertEquals(1, entries.size());
        assertEquals(expectedTimestamp, entries.get(0).timestamp);
    }

    @Test
    public void testTimestampPreserved() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add entries with specific timestamps
        long baseTimestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            long timestamp = baseTimestamp + (i * 1000);
            memtable.put(new ByteArrayWrapper(key), new Value(value, timestamp, false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        // Verify timestamps are preserved
        for (int i = 0; i < entries.size(); i++) {
            long expectedTimestamp = baseTimestamp + (i * 1000);
            assertEquals(expectedTimestamp, entries.get(i).timestamp, 
                        "Timestamp should be preserved for entry " + i);
        }
    }

    @Test
    public void testMultipleTimestamps() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Add entries with varying timestamps
        long[] timestamps = {
            1700000000000L,
            1700000001000L,
            1700000002500L,
            1700000005000L,
            1700000010000L
        };
        
        for (int i = 0; i < timestamps.length; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, timestamps[i], false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(timestamps[i], entries.get(i).timestamp);
        }
    }

    // ============================================================
    // 7. FILE FORMAT TESTS
    // ============================================================

    @Test
    public void testFileStructure() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        for (int i = 0; i < 50; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        String filePath = getLastSSTable(0).getFilePath();
        
        // Verify file structure is valid
        assertTrue(SSTableInspector.validateMetadata(filePath), 
                  "File structure should be valid: entries -> index -> footer");
    }

    @Test
    public void testMagicNumbers() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        for (int i = 0; i < 10; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        String filePath = getLastSSTable(0).getFilePath();
        
        // Try to read with SSTableReader - it validates magic numbers
        try (SSTableReader reader = new SSTableReader(filePath)) {
            assertNotNull(reader, "Reader should successfully open file with valid magic numbers");
        }
    }

    @Test
    public void testFileNotCorrupted() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        for (int i = 0; i < 30; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        String filePath = getLastSSTable(0).getFilePath();
        
        // Verify all data can be read back correctly
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(filePath);
        assertEquals(30, entries.size());
        
        // Verify content integrity
        for (int i = 0; i < entries.size(); i++) {
            String expectedKey = "key_" + String.format("%05d", i);
            String expectedValue = "value_" + String.format("%05d", i);
            assertEquals(expectedKey, entries.get(i).getKeyAsString());
            assertEquals(expectedValue, entries.get(i).getValueAsString());
        }
    }

    // ============================================================
    // 8. EDGE CASES TESTS
    // ============================================================

    @Test
    public void testLargeValue() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Create entry with 1MB value
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] largeValue = TestDataGenerator.generateLargeValue(1024 * 1024, 1);
        
        memtable.put(new ByteArrayWrapper(key), new Value(largeValue, System.currentTimeMillis(), false));
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        assertEquals(1, entries.size());
        assertEquals(1024 * 1024, entries.get(0).value.length, "Large value should be written correctly");
    }

    @Test
    public void testEmptyValue() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Write entry with empty byte array value
        byte[] key = TestDataGenerator.generateKey(1);
        byte[] emptyValue = new byte[0];
        
        memtable.put(new ByteArrayWrapper(key), new Value(emptyValue, System.currentTimeMillis(), false));
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        assertEquals(1, entries.size());
        assertNotNull(entries.get(0).value);
        assertEquals(0, entries.get(0).value.length, "Empty value should be written correctly");
    }

    @Test
    public void testEmptyKey() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Try to write entry with empty key
        byte[] emptyKey = new byte[0];
        byte[] value = TestDataGenerator.generateValue(1);
        
        // This might work or might fail depending on implementation
        memtable.put(new ByteArrayWrapper(emptyKey), new Value(value, System.currentTimeMillis(), false));
        
        SSTableWriter writer = new SSTableWriter(0);
        writer.write(memtable);
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableInspector.SSTableEntry> entries = SSTableInspector.readAllEntries(getLastSSTable(0).getFilePath());
        
        assertEquals(1, entries.size());
        assertEquals(0, entries.get(0).key.length);
    }

    @Test
    public void testManyEntries() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Write 100K entries - test scalability
        int entryCount = 100_000;
        
        // This might exceed memtable threshold, so we'll use a smaller number for practical testing
        // Use 10K instead to keep test reasonable
        int practicalCount = 10_000;
        
        for (int i = 0; i < practicalCount; i++) {
            byte[] key = TestDataGenerator.generateKey(i);
            byte[] value = TestDataGenerator.generateValue(i);
            memtable.put(new ByteArrayWrapper(key), new Value(value, System.currentTimeMillis(), false));
        }
        
        SSTableWriter writer = new SSTableWriter(0);
        long startTime = System.currentTimeMillis();
        writer.write(memtable);
        long duration = System.currentTimeMillis() - startTime;
        
        System.out.println("Wrote " + practicalCount + " entries in " + duration + "ms");
        
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(0);
        int actualCount = SSTableInspector.getKeyCount(getLastSSTable(0).getFilePath());
        
        assertEquals(practicalCount, actualCount, "Should write all entries");
        
        // Verify file is not corrupt
        assertTrue(SSTableInspector.validateMetadata(getLastSSTable(0).getFilePath()));
    }

    // ============================================================
    // HELPER METHODS
    // ============================================================

    /**
     * Get the most recently created SSTable at level 0.
     * This is needed because TableDirectory is a singleton that accumulates across tests.
     */
    private SSTableMetadata getLastSSTable(int level) {
        List<SSTableMetadata> sstables = tableDirectory.getSSTablesAtLevel(level);
        if (sstables.isEmpty()) {
            throw new IllegalStateException("No SSTables found at level " + level);
        }
        return sstables.get(sstables.size() - 1);
    }

    private void cleanDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        
        Files.walk(dir)
             .filter(path -> !path.equals(dir))
             .sorted(Comparator.reverseOrder())
             .forEach(path -> {
                 try {
                     Files.deleteIfExists(path);
                 } catch (IOException e) {
                     // Ignore
                 }
             });
    }

    private void deleteDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }
        
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        
        directory.delete();
    }
}

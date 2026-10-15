package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.Memtable;
import org.lsmtdb.core.sstable.SSTableWriter;
import org.lsmtdb.test.utils.LSMTreeTestHarness;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

abstract class RealCompactionPathIntegrationBaseTest {

    protected LSMTreeTestHarness harness;

    @BeforeEach
    void setUpRealPathHarness() throws IOException {
        LevelMetadata.setLevelBaseMaxSizeForTests(1L);
        harness = LSMTreeTestHarness.createTestDatabase(getClass().getSimpleName());
    }

    @AfterEach
    void tearDownRealPathHarness() throws IOException {
        LevelMetadata.resetLevelBaseMaxSizeForTests();
        if (harness != null) {
            harness.close();
        }
    }

    protected void flushSingleEntryToL0(String key, String value, long timestamp) throws IOException {
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
        memtable.put(new ByteArrayWrapper(key.getBytes(StandardCharsets.UTF_8)),
                new Value(value.getBytes(StandardCharsets.UTF_8), timestamp, false));
        try (SSTableWriter writer = new SSTableWriter(0)) {
            writer.write(memtable);
        }
        memtable.clear();
    }

    /**
     * Flush multiple entries to a single L0 SSTable.
     * This enables creating SSTables with overlapping key ranges for merge testing.
     *
     * @param entries Map of key→value pairs to write
     * @param timestamp Timestamp for all entries in this SSTable
     */
    protected void flushMultipleEntriesToL0(Map<String, String> entries, long timestamp) throws IOException {
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            memtable.put(
                    new ByteArrayWrapper(entry.getKey().getBytes(StandardCharsets.UTF_8)),
                    new Value(entry.getValue().getBytes(StandardCharsets.UTF_8), timestamp, false)
            );
        }
        try (SSTableWriter writer = new SSTableWriter(0)) {
            writer.write(memtable);
        }
        memtable.clear();
    }

    /**
     * Flush multiple entries with individual timestamps to a single L0 SSTable.
     * Useful for testing timestamp precedence within a single SSTable.
     *
     * @param entries Map of key→(value, timestamp) pairs
     */
    protected void flushMultipleEntriesWithTimestampsToL0(Map<String, ValueWithTimestamp> entries) throws IOException {
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
        for (Map.Entry<String, ValueWithTimestamp> entry : entries.entrySet()) {
            ValueWithTimestamp vt = entry.getValue();
            memtable.put(
                    new ByteArrayWrapper(entry.getKey().getBytes(StandardCharsets.UTF_8)),
                    new Value(vt.value.getBytes(StandardCharsets.UTF_8), vt.timestamp, false)
            );
        }
        try (SSTableWriter writer = new SSTableWriter(0)) {
            writer.write(memtable);
        }
        memtable.clear();
    }

    /**
     * Flush a tombstone for the given key to L0.
     */
    protected void flushTombstoneToL0(String key, long timestamp) throws IOException {
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
        memtable.put(
                new ByteArrayWrapper(key.getBytes(StandardCharsets.UTF_8)),
                new Value(null, timestamp, true)
        );
        try (SSTableWriter writer = new SSTableWriter(0)) {
            writer.write(memtable);
        }
        memtable.clear();
    }

    /**
     * Helper to run compaction until L0 is empty.
     */
    protected void compactUntilL0Empty(CompactionManager manager, int maxAttempts) throws InterruptedException {
        for (int i = 0; i < maxAttempts; i++) {
            manager.checkAndTriggerCompaction();
            if (harness.getSSTableCount(0) == 0) {
                return;
            }
            Thread.sleep(15L);
        }
        fail("Compaction did not drain L0 within bounded polling attempts");
    }

    /**
     * Helper to run compaction until state stabilizes.
     */
    protected void compactUntilStable(CompactionManager manager, int maxAttempts) throws InterruptedException {
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            List<LevelMetadata> before = harness.getAllLevels();
            manager.checkAndTriggerCompaction();
            Thread.sleep(20L);
            List<LevelMetadata> after = harness.getAllLevels();
            if (sameState(before, after)) {
                return;
            }
        }
    }

    protected static ByteArrayWrapper key(String value) {
        return new ByteArrayWrapper(value.getBytes(StandardCharsets.UTF_8));
    }

    protected static byte[] raw(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    protected record ValueWithTimestamp(String value, long timestamp) {}

    protected void pollCompactionWithBoundedChecks(CompactionManager manager, int maxAttempts) throws InterruptedException {
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            List<LevelMetadata> before = harness.getAllLevels();
            manager.checkAndTriggerCompaction();
            Thread.sleep(10L);
            List<LevelMetadata> after = harness.getAllLevels();
            if (sameState(before, after)) {
                return;
            }
        }
        fail("Compaction did not stabilize within bounded polling attempts");
    }

    private boolean sameState(List<LevelMetadata> left, List<LevelMetadata> right) {
        if (left.size() != right.size()) {
            return false;
        }
        Map<Integer, LevelMetadata> leftByLevel = left.stream()
                .collect(Collectors.toMap(LevelMetadata::getLevelNumber, level -> level));
        Map<Integer, LevelMetadata> rightByLevel = right.stream()
                .collect(Collectors.toMap(LevelMetadata::getLevelNumber, level -> level));

        if (!leftByLevel.keySet().equals(rightByLevel.keySet())) {
            return false;
        }

        for (Integer levelNumber : leftByLevel.keySet()) {
            LevelMetadata a = leftByLevel.get(levelNumber);
            LevelMetadata b = rightByLevel.get(levelNumber);
            if (a == null || b == null) {
                return false;
            }
            if (a.getSstables().size() != b.getSstables().size()) {
                return false;
            }
            if (a.getTotalSize() != b.getTotalSize()) {
                return false;
            }
        }
        return true;
    }
}

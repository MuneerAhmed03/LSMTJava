package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.Test;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.Memtable;
import org.lsmtdb.core.sstable.NotFoundException;
import org.lsmtdb.core.sstable.SSTableSearch;
import org.lsmtdb.core.sstable.SSTableWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RealCompactionPathScenarioMatrixTest extends RealCompactionPathIntegrationBaseTest {

    @Test
    void earlyStableKeyReachesDeeperLevelAndSurvivesManyLaterWrites() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            flushSingleEntryToL0("stable-key", "stable-value", 10L);
            compactUntilL0Empty(manager, 100);

            for (int i = 0; i < 18; i++) {
                flushSingleEntryToL0("late-key-" + i, "late-value-" + i, 1_000L + i);
                compactUntilL0Empty(manager, 80);
            }

            byte[] actual = new SSTableSearch().search(key("stable-key"));
            assertArrayEquals(raw("stable-value"), actual,
                    "Stable early key must remain retrievable after many later flush/compaction rounds");
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void latestWriteWinsAcrossMultipleFlushAndCompactionRounds() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            flushSingleEntryToL0("versioned-key", "v1", 100L);
            flushSingleEntryToL0("noise-a", "noise-a", 101L);
            compactUntilL0Empty(manager, 100);

            flushSingleEntryToL0("versioned-key", "v2", 200L);
            flushSingleEntryToL0("noise-b", "noise-b", 201L);
            compactUntilL0Empty(manager, 100);

            flushSingleEntryToL0("noise-c", "noise-c", 202L);
            compactUntilL0Empty(manager, 80);

            byte[] actual = new SSTableSearch().search(key("versioned-key"));
            assertArrayEquals(raw("v2"), actual,
                    "Newest write must win after repeated real-path flush and compaction rounds");
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void tombstoneShadowsAcrossLevelsBeforeAndAfterCompaction() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            flushSingleEntryToL0("delete-me", "live-value", 300L);
            compactUntilL0Empty(manager, 100);

            flushSingleTombstoneToL0("delete-me", 400L);

            assertThrows(NotFoundException.class,
                    () -> new SSTableSearch().search(key("delete-me")),
                    "Tombstone in higher-priority level must shadow deeper stale values before compaction");

            compactUntilL0Empty(manager, 100);

            assertThrows(NotFoundException.class,
                    () -> new SSTableSearch().search(key("delete-me")),
                    "Tombstone must continue to shadow older value after compaction rounds");
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void distinctKeysArePreservedAfterRealCompactionCycles() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            flushSingleEntryToL0("k-alpha", "v-alpha", 500L);
            flushSingleEntryToL0("k-beta", "v-beta", 501L);
            compactUntilL0Empty(manager, 100);
            flushSingleEntryToL0("k-gamma", "v-gamma", 502L);
            flushSingleEntryToL0("k-delta", "v-delta", 503L);
            compactUntilL0Empty(manager, 100);

            assertArrayEquals(raw("v-alpha"), new SSTableSearch().search(key("k-alpha")));
            assertArrayEquals(raw("v-beta"), new SSTableSearch().search(key("k-beta")));
            assertArrayEquals(raw("v-gamma"), new SSTableSearch().search(key("k-gamma")));
            assertArrayEquals(raw("v-delta"), new SSTableSearch().search(key("k-delta")));
        } finally {
            manager.shutdown();
        }
    }

    private void flushSingleTombstoneToL0(String key, long timestamp) throws IOException {
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
        memtable.put(key(key), new Value(null, timestamp, true));
        try (SSTableWriter writer = new SSTableWriter(0)) {
            writer.write(memtable);
        }
        memtable.clear();
    }

}

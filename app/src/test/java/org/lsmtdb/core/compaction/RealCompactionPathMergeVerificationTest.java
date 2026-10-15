package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.Test;
import org.lsmtdb.core.sstable.SSTableSearch;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RealCompactionPathMergeVerificationTest extends RealCompactionPathIntegrationBaseTest {

    @Test
    void multiEntryOverlappingMergeTest() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            // Create SSTable A with entries: {key1→v1, key2→v2, key3→v3} at timestamp 100
            Map<String, String> entriesA = new LinkedHashMap<>();
            entriesA.put("key1", "v1");
            entriesA.put("key2", "v2");
            entriesA.put("key3", "v3");
            flushMultipleEntriesToL0(entriesA, 100L);

            // Create SSTable B with entries: {key2→v4, key3→v5, key4→v6} at timestamp 200 (overlapping key2, key3)
            Map<String, String> entriesB = new LinkedHashMap<>();
            entriesB.put("key2", "v4");
            entriesB.put("key3", "v5");
            entriesB.put("key4", "v6");
            flushMultipleEntriesToL0(entriesB, 200L);

            // Trigger compaction
            compactUntilL0Empty(manager, 100);

            // Assert that searching returns: key2→v4, key3→v5 (newer timestamps win)
            assertArrayEquals(raw("v4"), new SSTableSearch().search(key("key2")),
                    "key2 should have value v4 from newer timestamp");
            assertArrayEquals(raw("v5"), new SSTableSearch().search(key("key3")),
                    "key3 should have value v5 from newer timestamp");

            // Assert all 4 distinct keys are retrievable
            assertArrayEquals(raw("v1"), new SSTableSearch().search(key("key1")),
                    "key1 should be retrievable");
            assertArrayEquals(raw("v6"), new SSTableSearch().search(key("key4")),
                    "key4 should be retrievable");
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void deepLevelL2VerificationTest() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            // Flush SSTables in batches with compaction between each batch to push data deeper
            for (int batch = 0; batch < 10; batch++) {
                for (int i = 0; i < 5; i++) {
                    int keyIndex = batch * 5 + i;
                    flushSingleEntryToL0("deep-key-" + keyIndex, "deep-value-" + keyIndex, 1000L + keyIndex);
                }
                // Compact after each batch to push data through levels
                compactUntilL0Empty(manager, 100);
            }

            // Run additional compaction rounds to push data to L2
            compactUntilStable(manager, 300);

            // Assert data reached L2
            assertTrue(harness.getSSTableCount(2) > 0,
                    "Data should have reached L2 after compacting many SSTables");

            // Verify an early key is still retrievable
            assertArrayEquals(raw("deep-value-0"), new SSTableSearch().search(key("deep-key-0")),
                    "Early key should still be retrievable after reaching L2");
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void multiL0TransitiveOverlapExpansionTest() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            // Create SSTable A with keys [aaa, bbb, ccc, ddd]
            Map<String, String> entriesA = new LinkedHashMap<>();
            entriesA.put("aaa", "valueA1");
            entriesA.put("bbb", "valueA2");
            entriesA.put("ccc", "valueA3");
            entriesA.put("ddd", "valueA4");
            flushMultipleEntriesToL0(entriesA, 100L);

            // Create SSTable B with keys [ccc, ddd, eee, fff] (overlaps A)
            Map<String, String> entriesB = new LinkedHashMap<>();
            entriesB.put("ccc", "valueB1");
            entriesB.put("ddd", "valueB2");
            entriesB.put("eee", "valueB3");
            entriesB.put("fff", "valueB4");
            flushMultipleEntriesToL0(entriesB, 200L);

            // Create SSTable C with keys [eee, fff, ggg, hhh] (overlaps B, transitively overlaps A)
            Map<String, String> entriesC = new LinkedHashMap<>();
            entriesC.put("eee", "valueC1");
            entriesC.put("fff", "valueC2");
            entriesC.put("ggg", "valueC3");
            entriesC.put("hhh", "valueC4");
            flushMultipleEntriesToL0(entriesC, 300L);

            assertEquals(3, harness.getSSTableCount(0), "Should have 3 SSTables in L0 before compaction");

            // Trigger single compaction round
            compactUntilL0Empty(manager, 100);

            // Verify all L0 SSTables consumed (L0 count = 0)
            assertEquals(0, harness.getSSTableCount(0),
                    "All L0 SSTables should be consumed after compaction");

            // Verify all keys retrievable from L1 (with correct timestamps winning)
            assertArrayEquals(raw("valueA1"), new SSTableSearch().search(key("aaa")),
                    "aaa should be retrievable");
            assertArrayEquals(raw("valueA2"), new SSTableSearch().search(key("bbb")),
                    "bbb should be retrievable");
            assertArrayEquals(raw("valueB1"), new SSTableSearch().search(key("ccc")),
                    "ccc should have newer value from SSTable B");
            assertArrayEquals(raw("valueB2"), new SSTableSearch().search(key("ddd")),
                    "ddd should have newer value from SSTable B");
            assertArrayEquals(raw("valueC1"), new SSTableSearch().search(key("eee")),
                    "eee should have newest value from SSTable C");
            assertArrayEquals(raw("valueC2"), new SSTableSearch().search(key("fff")),
                    "fff should have newest value from SSTable C");
            assertArrayEquals(raw("valueC3"), new SSTableSearch().search(key("ggg")),
                    "ggg should be retrievable");
            assertArrayEquals(raw("valueC4"), new SSTableSearch().search(key("hhh")),
                    "hhh should be retrievable");
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void l1ToL2CompactionTest() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            // Push enough data to fill L1 past maxSize
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 10; i++) {
                    String key = "l1tol2-batch" + batch + "-key" + i;
                    String value = "l1tol2-batch" + batch + "-value" + i;
                    flushSingleEntryToL0(key, value, 1000L + (batch * 100) + i);
                }
                compactUntilL0Empty(manager, 100);
            }

            // Trigger compaction repeatedly to push data to L2
            compactUntilStable(manager, 200);

            // Assert data appears in L2
            assertTrue(harness.getSSTableCount(2) > 0,
                    "Data should appear in L2 after sufficient L1 compaction");

            // Assert keys still retrievable
            assertArrayEquals(raw("l1tol2-batch0-value0"), new SSTableSearch().search(key("l1tol2-batch0-key0")),
                    "Early batch key should still be retrievable after L1→L2 compaction");
            assertArrayEquals(raw("l1tol2-batch4-value9"), new SSTableSearch().search(key("l1tol2-batch4-key9")),
                    "Late batch key should still be retrievable after L1→L2 compaction");
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void timestampPrecedenceDuringMergeTest() throws Exception {
        CompactionManager manager = harness.createCompactionManager();
        try {
            // Create 3 SSTables all containing same key "shared-key":
            // SSTable A: shared-key→valueA at timestamp 100
            flushSingleEntryToL0("shared-key", "valueA", 100L);

            // SSTable B: shared-key→valueB at timestamp 300
            flushSingleEntryToL0("shared-key", "valueB", 300L);

            // SSTable C: shared-key→valueC at timestamp 200
            flushSingleEntryToL0("shared-key", "valueC", 200L);

            assertEquals(3, harness.getSSTableCount(0), "Should have 3 SSTables in L0 before compaction");

            // Trigger merge compaction
            compactUntilL0Empty(manager, 100);

            // Assert search returns "valueB" (timestamp 300 wins)
            assertArrayEquals(raw("valueB"), new SSTableSearch().search(key("shared-key")),
                    "Value from timestamp 300 (valueB) should win during merge");
        } finally {
            manager.shutdown();
        }
    }
}

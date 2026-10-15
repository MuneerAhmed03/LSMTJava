package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.Test;
import org.lsmtdb.core.sstable.NotFoundException;
import org.lsmtdb.core.sstable.SSTableSearch;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spec-first tests that are EXPECTED TO FAIL because they expose bugs in the implementation.
 * 
 * These tests document known issues in the LSM tree implementation:
 * 1. L0 search order bug - searches oldest-to-newest instead of newest-to-oldest
 * 2. Tombstone elimination during compaction merge - may not properly suppress deleted keys
 * 
 * When these tests start passing, the corresponding bugs have been fixed.
 */
class RealCompactionPathBugExposureTest extends RealCompactionPathIntegrationBaseTest {

    /**
     * EXPOSES BUG: L0 search order is incorrect.
     * 
     * The current implementation in SSTableSearch.java (line 26) searches L0 SSTables
     * from index 0 to N using: for(int i = 0; i < levelCandidates.size(); i++)
     * 
     * This iterates in minKey-sorted order (oldest-to-newest when minKeys are equal),
     * returning the FIRST match found. LSM semantics require searching newest-to-oldest
     * so that more recent writes shadow older ones.
     * 
     * THIS TEST IS EXPECTED TO FAIL until the L0 search order bug is fixed.
     * The fix should iterate in reverse order (newest SSTable first) or sort L0 by
     * timestamp/sequence number descending.
     */
    @Test
    void l0SearchOrderBugExposureTest() throws Exception {
        // SSTable A: older entry for key "aaa"
        flushMultipleEntriesToL0(Map.of("aaa", "oldValue"), 100L);
        
        // SSTable B: newer entry for same key "aaa" 
        // Both SSTables have minKey="aaa", so A sorts first by minKey order
        flushMultipleEntriesToL0(Map.of("aaa", "newValue"), 200L);
        
        // Verify we have 2 L0 SSTables
        assertTrue(harness.getSSTableCount(0) >= 2, 
                "Should have at least 2 L0 SSTables for this test");
        
        // Search for "aaa" - should return "newValue" (timestamp 200)
        // BUG: Current implementation returns "oldValue" because it searches
        // from index 0 (SSTable A with older timestamp) first
        byte[] result = new SSTableSearch().search(key("aaa"));
        
        assertArrayEquals(raw("newValue"), result,
                "L0 search should return the NEWEST value for a key, not the oldest. " +
                "Expected 'newValue' (timestamp 200) but got 'oldValue' (timestamp 100). " +
                "This exposes the L0 search order bug in SSTableSearch.");
    }

    /**
     * VALIDATES: Tombstone properly eliminates key from merged output during compaction.
     * 
     * When a tombstone is merged with a live value during compaction, the key should
     * be completely eliminated from the output SSTable (or retained as tombstone only
     * until it propagates to the bottom level).
     * 
     * The current implementation may fail because:
     * - SSTableMerger line 66: `if (value != null)` skips tombstones but may not
     *   properly suppress the key from appearing in output
     * - Or the search continues to deeper levels and finds stale values
     * 
     * THIS TEST VERIFIES CORRECT TOMBSTONE SEMANTICS.
     * It may fail if tombstone elimination during merge is not properly implemented.
     */
    @Test
    void tombstoneEliminationFromMergedOutputTest() throws Exception {
        // Step 1: Create live value and compact to L1
        flushSingleEntryToL0("delete-me", "liveValue", 100L);
        
        CompactionManager manager = harness.createCompactionManager();
        
        // Compact until L0 is empty - this moves the entry to L1
        compactUntilL0Empty(manager, 30);
        
        // Verify the key exists before tombstone
        byte[] beforeDelete = new SSTableSearch().search(key("delete-me"));
        assertArrayEquals(raw("liveValue"), beforeDelete,
                "Key should be searchable before tombstone is applied");
        
        // Step 2: Flush a tombstone for the same key with newer timestamp
        flushTombstoneToL0("delete-me", 200L);
        
        // Step 3: Compact L0 (tombstone) with L1 (live value) -> merged L1
        // The tombstone (timestamp 200) should shadow the live value (timestamp 100)
        compactUntilL0Empty(manager, 30);
        
        // Allow compaction to fully stabilize
        compactUntilStable(manager, 30);
        
        manager.shutdown();
        
        // Step 4: Search for the deleted key - should throw NotFoundException
        // If tombstone elimination works correctly, the key is either:
        // - Completely removed from the merged SSTable, OR
        // - Present as a tombstone that the search correctly interprets as "not found"
        assertThrows(NotFoundException.class, 
                () -> new SSTableSearch().search(key("delete-me")),
                "After tombstone compaction, searching for 'delete-me' should throw NotFoundException. " +
                "This verifies that tombstones properly eliminate keys during the merge process.");
    }
}

package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.SSTableMetadata;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompactionStrategyTest {

    private final CompactionStrategy strategy = new CompactionStrategy();

    @AfterEach
    void resetThresholdOverrides() {
        LevelMetadata.resetLevelBaseMaxSizeForTests();
    }

    @Test
    void findL0Overlaps_expandsTransitively() {
        SSTableMetadata a = meta(1, "a", "d", 0);
        SSTableMetadata b = meta(2, "c", "f", 0);
        SSTableMetadata c = meta(3, "f", "k", 0);
        SSTableMetadata d = meta(4, "z", "zz", 0);

        List<SSTableMetadata> overlaps = strategy.findl0Overlaps(a, List.of(a, b, c, d));

        assertEquals(3, overlaps.size());
        assertTrue(overlaps.contains(a));
        assertTrue(overlaps.contains(b));
        assertTrue(overlaps.contains(c));
    }

    @Test
    void findOverlapsWithinRange_returnsOnlyIntersectingFiles() {
        SSTableMetadata l1a = meta(10, "a", "f", 1);
        SSTableMetadata l1b = meta(11, "g", "m", 1);
        SSTableMetadata l1c = meta(12, "n", "z", 1);

        List<SSTableMetadata> overlaps = strategy.findOverlapsWithinRange(
                key("c"), key("h"), List.of(l1a, l1b, l1c));

        assertEquals(2, overlaps.size());
        assertTrue(overlaps.contains(l1a));
        assertTrue(overlaps.contains(l1b));
    }

    @Test
    void levelMaxSizeUsesDefaultWhenNoOverrideProvided() {
        LevelMetadata metadata = new LevelMetadata(0);
        assertEquals(LevelMetadata.getDefaultLevelBaseMaxSizeBytes(), metadata.getMaxSize());
    }

    @Test
    void levelMaxSizeOverrideForTestsChangesComputedMaxSize() {
        LevelMetadata.setLevelBaseMaxSizeForTests(128);
        LevelMetadata metadata = new LevelMetadata(2);
        assertEquals(12800, metadata.getMaxSize());
    }

    @Test
    void levelMaxSizeOverrideRejectsNonPositiveValues() {
        assertThrows(IllegalArgumentException.class, () -> LevelMetadata.setLevelBaseMaxSizeForTests(0));
        assertThrows(IllegalArgumentException.class, () -> LevelMetadata.setLevelBaseMaxSizeForTests(-10));
    }

    private static SSTableMetadata meta(int fileNo, String min, String max, int level) {
        return new SSTableMetadata(fileNo, "/tmp/" + fileNo + ".sst", key(min), key(max), 100, false, level);
    }

    private static ByteArrayWrapper key(String s) {
        return new ByteArrayWrapper(s.getBytes(StandardCharsets.UTF_8));
    }
}

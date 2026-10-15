package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lsmtdb.common.AppConstants;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.Memtable;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.SSTableSearch;
import org.lsmtdb.core.sstable.SSTableWriter;
import org.lsmtdb.core.sstable.TableDirectory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompactionManagerSpecTest {

    private Path dbRoot;

    @BeforeEach
    void setUp() throws IOException {
        LevelMetadata.resetLevelBaseMaxSizeForTests();
        dbRoot = Paths.get(AppConstants.BASE_DB_PATH);
        deleteRecursively(dbRoot);
        Files.createDirectories(dbRoot);

        TableDirectory.resetForTests();
        TableDirectory.getInstance();
        Memtable.getInstance().clear();
    }

    @AfterEach
    void tearDown() throws IOException {
        Memtable.getInstance().clear();
        LevelMetadata.resetLevelBaseMaxSizeForTests();
        TableDirectory.resetForTests();
        deleteRecursively(dbRoot);
    }

    @Test
    void noOverlapPromotionMovesL0FileToL1AndRemovesOldMetadata() throws Exception {
        writeSingleEntrySSTable(0, "alpha", "one", 100L);

        TableDirectory tableDirectory = TableDirectory.getInstance();
        SSTableMetadata originalL0 = tableDirectory.getSSTablesAtLevel(0).get(0);
        String oldFilePath = originalL0.getFilePath();
        int oldFileNumber = originalL0.getFileNumber();

        new CompactionManager().handleNoOverlap(originalL0);

        List<SSTableMetadata> l0Files = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableMetadata> l1Files = tableDirectory.getSSTablesAtLevel(1);

        assertFalse(l0Files.stream().anyMatch(m -> m.getFileNumber() == oldFileNumber),
                "L0 metadata must remove the promoted file");
        assertEquals(1, l1Files.size(), "L1 must contain the promoted SSTable");
        assertTrue(Files.exists(Paths.get(l1Files.get(0).getFilePath())), "Promoted L1 file must exist");
        assertFalse(Files.exists(Paths.get(oldFilePath)), "Original L0 file must be removed");
    }

    @Test
    void levelMetadataReflectsExpectedCountsAndLevelsAfterPromotion() throws Exception {
        writeSingleEntrySSTable(0, "beta", "two", 200L);

        TableDirectory tableDirectory = TableDirectory.getInstance();
        int l0Before = tableDirectory.getSSTablesAtLevel(0).size();
        int l1Before = tableDirectory.getSSTablesAtLevel(1).size();

        SSTableMetadata source = tableDirectory.getSSTablesAtLevel(0).get(0);
        new CompactionManager().handleNoOverlap(source);

        List<SSTableMetadata> l0After = tableDirectory.getSSTablesAtLevel(0);
        List<SSTableMetadata> l1After = tableDirectory.getSSTablesAtLevel(1);

        assertEquals(l0Before - 1, l0After.size(), "L0 file count should decrease by one");
        assertEquals(l1Before + 1, l1After.size(), "L1 file count should increase by one");

        SSTableMetadata promoted = l1After.stream()
                .max(Comparator.comparingInt(SSTableMetadata::getFileNumber))
                .orElse(null);
        assertNotNull(promoted, "Promoted SSTable metadata must exist in L1");
        assertEquals(1, promoted.getLevel(), "Promoted file must be recorded at level 1");
    }

    @Test
    void promotionPreservesKeyRetrieval() throws Exception {
        writeSingleEntrySSTable(0, "gamma", "three", 300L);

        TableDirectory tableDirectory = TableDirectory.getInstance();
        SSTableMetadata source = tableDirectory.getSSTablesAtLevel(0).get(0);
        new CompactionManager().handleNoOverlap(source);

        byte[] actual = new SSTableSearch().search(new ByteArrayWrapper("gamma".getBytes(StandardCharsets.UTF_8)));
        assertArrayEquals("three".getBytes(StandardCharsets.UTF_8), actual,
                "Key lookup must return the same value after no-overlap promotion");
    }

    @Test
    void earlyKeyReachingL2RemainsRetrievableAfterManyNewWrites() throws Exception {
        TableDirectory tableDirectory = TableDirectory.getInstance();
        CompactionManager manager = new CompactionManager();

        writeSingleEntrySSTable(0, "early-key", "early-value", 10L);
        SSTableMetadata earlyL0 = tableDirectory.getSSTablesAtLevel(0).get(0);
        manager.handleNoOverlap(earlyL0); // L0 -> L1

        SSTableMetadata earlyL1 = tableDirectory.getSSTablesAtLevel(1).stream()
                .filter(m -> "early-key".equals(new String(m.getMinKey().getData(), StandardCharsets.UTF_8)))
                .findFirst()
                .orElseThrow();
        manager.handleNoOverlap(earlyL1); // L1 -> L2

        for (int i = 0; i < 40; i++) {
            writeSingleEntrySSTable(0, "new-key-" + i, "new-value-" + i, 1000L + i);
            SSTableMetadata newestL0 = tableDirectory.getSSTablesAtLevel(0).stream()
                    .max(Comparator.comparingInt(SSTableMetadata::getFileNumber))
                    .orElseThrow();
            manager.handleNoOverlap(newestL0); // keep injecting newer data above/beside early key
        }

        byte[] actual = new SSTableSearch().search(new ByteArrayWrapper("early-key".getBytes(StandardCharsets.UTF_8)));
        assertArrayEquals("early-value".getBytes(StandardCharsets.UTF_8), actual,
                "A key written early and later resident in L2 must still return its original value if never updated/deleted");
    }

    @Test
    void checkAndTriggerCompactionUsesTinyTestThresholdToCompactSmallDataset() throws Exception {
        LevelMetadata.setLevelBaseMaxSizeForTests(1);
        deleteRecursively(dbRoot);
        Files.createDirectories(dbRoot);
        TableDirectory.resetForTests();
        TableDirectory tableDirectory = TableDirectory.getInstance();
        CompactionManager manager = new CompactionManager();
        try {
            writeSingleEntrySSTable(0, "tiny-key", "tiny-value", 1L);
            assertTrue(tableDirectory.getLevelMetadata(0).getTotalSize() > tableDirectory.getLevelMetadata(0).getMaxSize(),
                    "Tiny test threshold must mark small L0 dataset as eligible for compaction");

            manager.checkAndTriggerCompaction();

            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline && tableDirectory.getSSTablesAtLevel(0).size() > 0) {
                Thread.sleep(20);
            }

            assertTrue(tableDirectory.getSSTablesAtLevel(0).isEmpty(),
                    "L0 should be compacted away under tiny test threshold");
            assertFalse(tableDirectory.getSSTablesAtLevel(1).isEmpty(),
                    "Compaction should create output SSTable in L1");
        } finally {
            manager.shutdown();
        }
    }

    private static void writeSingleEntrySSTable(int level, String key, String value, long timestamp) throws Exception {
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
        memtable.put(new ByteArrayWrapper(key.getBytes(StandardCharsets.UTF_8)),
                new Value(value.getBytes(StandardCharsets.UTF_8), timestamp, false));

        try (SSTableWriter writer = new SSTableWriter(level)) {
            writer.write(memtable);
        }

        memtable.clear();
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }

        Files.walk(root)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                    }
                });
    }
}

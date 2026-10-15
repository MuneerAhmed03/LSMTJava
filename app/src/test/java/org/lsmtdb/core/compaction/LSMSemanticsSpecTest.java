package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lsmtdb.common.AppConstants;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.Memtable;
import org.lsmtdb.core.sstable.NotFoundException;
import org.lsmtdb.core.sstable.SSTableSearch;
import org.lsmtdb.core.sstable.SSTableWriter;
import org.lsmtdb.core.sstable.TableDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Spec-first LSM tests: these assert expected LSM semantics, even if current implementation is buggy.
 */
class LSMSemanticsSpecTest {
    private Path tempDbDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDbDir = Path.of("build", "test-dbs", "lsm-spec-" + System.nanoTime()).toAbsolutePath();
        Files.createDirectories(tempDbDir);
        AppConstants.setBaseDirForTests(tempDbDir.toString());
        TableDirectory.resetForTests();
        TableDirectory.getInstance();
        Memtable.getInstance().clear();
    }

    @AfterEach
    void tearDown() throws IOException {
        Memtable.getInstance().clear();
        TableDirectory.resetForTests();
        AppConstants.resetBaseDirForTests();
        if (tempDbDir != null && Files.exists(tempDbDir)) {
            Files.walk(tempDbDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        }
    }

    @Test
    void l0LookupMustReturnNewestVersionForDuplicateKey() throws Exception {
        byte[] key = "user:42".getBytes();
        writeSingleEntrySSTable(0, key, "old-value".getBytes(), 100L, false);
        writeSingleEntrySSTable(0, key, "new-value".getBytes(), 200L, false);

        byte[] actual = new SSTableSearch().search(new ByteArrayWrapper(key));
        assertArrayEquals("new-value".getBytes(), actual,
                "LSM lookup must prefer the newest L0 version for duplicate keys");
    }

    @Test
    void l0TombstoneMustShadowOlderValue() throws Exception {
        byte[] key = "order:99".getBytes();
        writeSingleEntrySSTable(0, key, "live".getBytes(), 100L, false);
        writeSingleEntrySSTable(0, key, null, 200L, true);

        assertThrows(NotFoundException.class,
                () -> new SSTableSearch().search(new ByteArrayWrapper(key)),
                "A newer tombstone in L0 must hide older values");
    }

    @Test
    void higherLevelTombstoneMustShadowDeeperOlderValue() throws Exception {
        byte[] key = "session:abc".getBytes();
        writeSingleEntrySSTable(2, key, "stale".getBytes(), 100L, false);
        writeSingleEntrySSTable(1, key, null, 200L, true);

        assertThrows(NotFoundException.class,
                () -> new SSTableSearch().search(new ByteArrayWrapper(key)),
                "Tombstone at a higher-priority level must hide deeper stale values");
    }

    private static void writeSingleEntrySSTable(int level, byte[] key, byte[] value, long timestamp, boolean tombstone)
            throws Exception {
        Memtable memtable = Memtable.getInstance();
        memtable.clear();
        memtable.put(new ByteArrayWrapper(key), new Value(value, timestamp, tombstone));
        try (SSTableWriter writer = new SSTableWriter(level)) {
            writer.write(memtable);
        }
        memtable.clear();
    }
}

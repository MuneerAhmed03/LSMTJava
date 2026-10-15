package org.lsmtdb.core.compaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.Memtable;
import org.lsmtdb.core.sstable.NotFoundException;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.SSTableReader;
import org.lsmtdb.core.sstable.SSTableSearch;
import org.lsmtdb.core.sstable.SSTableWriter;
import org.lsmtdb.core.sstable.TableDirectory;
import org.lsmtdb.core.sstable.merger.SSTableMerger;
import org.lsmtdb.test.utils.SSTableInspector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec-first tests for SSTable merger semantics.
 */
class SSTableMergerSemanticsSpecTest {
    private Path tempDbDir;
    private String originalUserDir;
    private TableDirectory tableDirectory;

    @BeforeEach
    void setUp() throws IOException {
        Path testBase = Paths.get("build", "tmp", "sstable-merger-semantics-spec");
        Files.createDirectories(testBase);
        tempDbDir = Files.createTempDirectory(testBase, "db-");

        originalUserDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDbDir.toString());

        TableDirectory.resetForTests();
        tableDirectory = TableDirectory.getInstance();
        Memtable.getInstance().clear();
    }

    @AfterEach
    void tearDown() throws IOException {
        Memtable.getInstance().clear();
        TableDirectory.resetForTests();
        if (originalUserDir != null) {
            System.setProperty("user.dir", originalUserDir);
        }

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
    void mergeKeepsNewestValueWhenKeyExistsInMultipleInputSSTables() throws Exception {
        byte[] key = bytes("user:7");
        SSTableMetadata older = writeSSTable(0, entry("user:7", "old-email", 100L, false));
        SSTableMetadata newer = writeSSTable(0, entry("user:7", "new-email", 200L, false));

        List<SSTableMetadata> merged = SSTableMerger.mergeSSTables(List.of(older, newer), 1);
        assertFalse(merged.isEmpty(), "Merge should produce at least one output SSTable");
        assertMergedValue(merged, key, bytes("new-email"));

        List<SSTableInspector.SSTableEntry> mergedEntries = readAllMergedEntries(merged);
        assertEquals(1, mergedEntries.size(),
                "Merged output should contain exactly one entry for a duplicate key");
        assertEquals("user:7", mergedEntries.get(0).getKeyAsString());
        assertEquals("new-email", mergedEntries.get(0).getValueAsString());
        assertEquals(200L, mergedEntries.get(0).timestamp);

        tableDirectory.removeSSTables(List.of(older, newer));
        tableDirectory.addSSTables(merged);
        byte[] searched = new SSTableSearch().search(new ByteArrayWrapper(key));
        assertArrayEquals(bytes("new-email"), searched,
                "Search against merged state must return the newest value");
    }

    @Test
    void mergeDropsKeyWhenNewestEntryIsTombstone() throws Exception {
        byte[] key = bytes("order:42");
        SSTableMetadata live = writeSSTable(0, entry("order:42", "paid", 100L, false));
        SSTableMetadata tombstone = writeSSTable(0, entry("order:42", null, 200L, true));

        List<SSTableMetadata> merged = SSTableMerger.mergeSSTables(List.of(live, tombstone), 1);
        assertFalse(merged.isEmpty(), "Merge should produce output metadata");
        assertMergedAbsent(merged, key);
        assertEquals(0, readAllMergedEntries(merged).size(),
                "Merged output should have no entries for this tombstoned-only fixture");
    }

    @Test
    void mergePreservesAllDistinctKeysAcrossMultipleInputFiles() throws Exception {
        SSTableMetadata first = writeSSTable(0, entry("alpha", "v1", 10L, false));
        SSTableMetadata second = writeSSTable(0, entry("beta", "v2", 20L, false));
        SSTableMetadata third = writeSSTable(0, entry("gamma", "v3", 30L, false));

        List<SSTableMetadata> merged = SSTableMerger.mergeSSTables(List.of(third, first, second), 1);
        assertFalse(merged.isEmpty(), "Merge should produce output metadata");
        assertMergedValue(merged, bytes("alpha"), bytes("v1"));
        assertMergedValue(merged, bytes("beta"), bytes("v2"));
        assertMergedValue(merged, bytes("gamma"), bytes("v3"));

        List<SSTableInspector.SSTableEntry> entries = readAllMergedEntries(merged);
        assertEquals(3, entries.size(), "All distinct keys from source files must be present");
        for (SSTableMetadata output : merged) {
            assertTrue(SSTableInspector.verifyKeyOrdering(output.getFilePath()),
                    "Merged SSTables should remain sorted by key");
        }
    }

    private SSTableMetadata writeSSTable(int level, EntrySpec... entries) throws Exception {
        Memtable memtable = Memtable.getInstance();
        memtable.clear();

        for (EntrySpec entry : entries) {
            memtable.put(new ByteArrayWrapper(bytes(entry.key())),
                    new Value(entry.value() == null ? null : bytes(entry.value()), entry.timestamp(), entry.tombstone()));
        }

        Set<Integer> before = new HashSet<>();
        for (SSTableMetadata metadata : tableDirectory.getSSTablesAtLevel(level)) {
            before.add(metadata.getFileNumber());
        }

        try (SSTableWriter writer = new SSTableWriter(level)) {
            writer.write(memtable);
        }

        memtable.clear();

        for (SSTableMetadata metadata : tableDirectory.getSSTablesAtLevel(level)) {
            if (!before.contains(metadata.getFileNumber())) {
                return metadata;
            }
        }

        throw new IllegalStateException("Failed to capture newly-written SSTable metadata");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static EntrySpec entry(String key, String value, long timestamp, boolean tombstone) {
        return new EntrySpec(key, value, timestamp, tombstone);
    }

    private void assertMergedValue(List<SSTableMetadata> mergedFiles, byte[] key, byte[] expectedValue) throws Exception {
        for (SSTableMetadata metadata : mergedFiles) {
            try (SSTableReader reader = new SSTableReader(metadata.getFilePath())) {
                byte[] actual = reader.get(key);
                assertArrayEquals(expectedValue, actual);
                return;
            } catch (NotFoundException ignored) {
            }
        }
        throw new AssertionError("Expected key not found in merged output: " + new String(key, StandardCharsets.UTF_8));
    }

    private void assertMergedAbsent(List<SSTableMetadata> mergedFiles, byte[] key) throws Exception {
        for (SSTableMetadata metadata : mergedFiles) {
            try (SSTableReader reader = new SSTableReader(metadata.getFilePath())) {
                byte[] value = reader.get(key);
                throw new AssertionError("Expected key to be absent but found value: " + new String(value, StandardCharsets.UTF_8));
            } catch (NotFoundException ignored) {
            }
        }
    }

    private List<SSTableInspector.SSTableEntry> readAllMergedEntries(List<SSTableMetadata> mergedFiles) throws IOException {
        return mergedFiles.stream()
                .flatMap(metadata -> {
                    try {
                        return SSTableInspector.readAllEntries(metadata.getFilePath()).stream();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private record EntrySpec(String key, String value, long timestamp, boolean tombstone) {
    }
}

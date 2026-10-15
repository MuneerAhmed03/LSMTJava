package org.lsmtdb.core.compaction;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.SSTableSearch;
import org.lsmtdb.core.sstable.TableDirectory;
import org.lsmtdb.test.utils.InvariantChecker;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealCompactionPathManifestTableDirectorySanityTest extends RealCompactionPathIntegrationBaseTest {

    @Test
    void manifestAndTableDirectoryStaySaneAcrossRealCompactionCycle() throws Exception {
        flushSingleEntryToL0("manifest-k1", "v1", 1L);
        flushSingleEntryToL0("manifest-k2", "v2", 2L);
        flushSingleEntryToL0("manifest-k3", "v3", 3L);

        TableDirectory tableDirectory = TableDirectory.getInstance();
        List<MetadataRecord> beforeCompaction = snapshotMetadata(tableDirectory);
        assertFalse(beforeCompaction.isEmpty(), "Fixture should create SSTables before compaction");

        CompactionManager manager = harness.createCompactionManager();
        try {
            triggerCompactionWithBoundedPolling(manager, 80);
        } finally {
            manager.shutdown();
        }

        waitUntilManifestMatchesMetadata();

        List<MetadataRecord> afterCompaction = snapshotMetadata(tableDirectory);
        assertFalse(afterCompaction.isEmpty(), "Compaction should leave at least one live SSTable");

        Set<Integer> liveFileNumbers = afterCompaction.stream()
                .map(MetadataRecord::fileNumber)
                .collect(Collectors.toSet());
        List<MetadataRecord> removedFromMetadata = beforeCompaction.stream()
                .filter(record -> !liveFileNumbers.contains(record.fileNumber()))
                .toList();
        assertFalse(removedFromMetadata.isEmpty(), "At least one compacted input should be removed from metadata");

        for (MetadataRecord removed : removedFromMetadata) {
            assertFalse(Files.exists(Path.of(removed.filePath())),
                    "Compacted input file must be deleted from disk: " + removed.filePath());
        }

        assertManifestAndDiskConsistency(tableDirectory);
        assertLevelAndFileMetadataSanity(tableDirectory);
    }

    @Test
    void metadataReloadConsistencyAfterCompactionAndSearchStillWorks() throws Exception {
        flushSingleEntryToL0("reload-k1", "rv1", 11L);
        flushSingleEntryToL0("reload-k2", "rv2", 12L);
        flushSingleEntryToL0("reload-k3", "rv3", 13L);

        CompactionManager manager = harness.createCompactionManager();
        try {
            triggerCompactionWithBoundedPolling(manager, 80);
        } finally {
            manager.shutdown();
        }

        TableDirectory tableDirectory = TableDirectory.getInstance();
        waitUntilManifestMatchesMetadata();
        Set<MetadataRecord> snapshotBeforeReload = new HashSet<>(snapshotMetadata(tableDirectory));

        TableDirectory.resetForTests();
        TableDirectory reloaded = TableDirectory.getInstance();
        waitUntilManifestMatchesMetadata();
        Set<MetadataRecord> snapshotAfterReload = new HashSet<>(snapshotMetadata(reloaded));

        assertEquals(snapshotBeforeReload, snapshotAfterReload,
                "Reloaded TableDirectory metadata must match pre-reload snapshot");

        assertArrayEquals("rv1".getBytes(StandardCharsets.UTF_8), search("reload-k1"));
        assertArrayEquals("rv2".getBytes(StandardCharsets.UTF_8), search("reload-k2"));
        assertArrayEquals("rv3".getBytes(StandardCharsets.UTF_8), search("reload-k3"));
    }

    private void assertManifestAndDiskConsistency(TableDirectory tableDirectory) throws IOException, InterruptedException {
        List<MetadataRecord> metadataRecords = snapshotMetadata(tableDirectory);
        Set<String> metadataPaths = metadataRecords.stream().map(MetadataRecord::filePath).collect(Collectors.toSet());

        Set<String> manifestPaths = readManifestRecords().stream()
                .map(ManifestRecord::filePath)
                .collect(Collectors.toSet());
        assertEquals(metadataPaths, manifestPaths,
                "Manifest entries should correspond exactly to in-memory metadata paths");

        Set<String> diskPaths = listSSTablePathsOnDisk(harness.getSSTablesPath());
        assertEquals(metadataPaths, diskPaths,
                "Live SSTable files on disk must appear in metadata and no stale metadata may remain");

        for (ManifestRecord manifestRecord : readManifestRecords()) {
            assertTrue(Files.exists(Path.of(manifestRecord.filePath())),
                    "Manifest entry should point to an existing SSTable file: " + manifestRecord.filePath());
        }
    }

    private void assertLevelAndFileMetadataSanity(TableDirectory tableDirectory) {
        InvariantChecker checker = new InvariantChecker(tableDirectory);
        for (LevelMetadata levelMetadata : tableDirectory.getAllLevels()) {
            int levelNumber = levelMetadata.getLevelNumber();
            long totalFromMetadata = 0L;
            long totalActualOnDisk = 0L;

            for (SSTableMetadata sstable : levelMetadata.getSstables()) {
                assertNotNull(sstable.getMinKey(), "SSTable minKey must be present");
                assertNotNull(sstable.getMaxKey(), "SSTable maxKey must be present");
                assertTrue(sstable.getMinKey().compareTo(sstable.getMaxKey()) <= 0,
                        "SSTable minKey must be <= maxKey");
                assertTrue(sstable.getFileNumber() > 0, "SSTable file number must be positive");
                assertEquals(levelNumber, sstable.getLevel(), "SSTable level should match containing level");
                assertTrue(sstable.getFilePath().contains("/L" + levelNumber + "/"),
                        "SSTable path should include its level directory");
                assertTrue(sstable.getFileSize() > 0, "SSTable metadata size must be positive");

                long actualSize;
                try {
                    actualSize = Files.size(Path.of(sstable.getFilePath()));
                } catch (IOException e) {
                    throw new AssertionError("Failed to inspect file size for " + sstable.getFilePath(), e);
                }
                assertTrue(actualSize >= sstable.getFileSize(),
                        "SSTable metadata fileSize should not exceed actual file size");
                totalFromMetadata += sstable.getFileSize();
                totalActualOnDisk += actualSize;
            }

            assertEquals(totalFromMetadata, levelMetadata.getTotalSize(),
                    "Level totalSize should equal sum of member SSTable sizes");
            assertTrue(totalActualOnDisk >= levelMetadata.getTotalSize(),
                    "Actual on-disk bytes for level should be at least metadata totalSize");
            assertTrue(checker.checkMetadataConsistency(levelNumber),
                    "Min/max metadata should match on-disk SSTable contents for level " + levelNumber);
        }
    }

    private Set<String> listSSTablePathsOnDisk(Path sstablesRoot) throws IOException {
        if (!Files.exists(sstablesRoot)) {
            return Set.of();
        }
        try (Stream<Path> stream = Files.walk(sstablesRoot)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".sst"))
                    .map(path -> path.toAbsolutePath().toString())
                    .collect(Collectors.toSet());
        }
    }

    private byte[] search(String key) throws IOException {
        return new SSTableSearch().search(new ByteArrayWrapper(key.getBytes(StandardCharsets.UTF_8)));
    }

    private List<MetadataRecord> snapshotMetadata(TableDirectory tableDirectory) {
        return tableDirectory.getAllLevels().stream()
                .sorted(Comparator.comparingInt(LevelMetadata::getLevelNumber))
                .flatMap(level -> level.getSstables().stream().map(sstable -> new MetadataRecord(
                        level.getLevelNumber(),
                        sstable.getFileNumber(),
                        Path.of(sstable.getFilePath()).toAbsolutePath().toString(),
                        sstable.getFileSize(),
                        keyString(sstable.getMinKey()),
                        keyString(sstable.getMaxKey()))))
                .toList();
    }

    private List<ManifestRecord> readManifestRecords() throws IOException {
        Path manifestPath = harness.getTestDbPath().resolve("manifest.json");
        if (!Files.exists(manifestPath)) {
            return List.of();
        }

        try (BufferedReader reader = Files.newBufferedReader(manifestPath)) {
            JsonObject manifest = JsonParser.parseReader(reader).getAsJsonObject();
            JsonArray levels = manifest.getAsJsonArray("levels");
            List<ManifestRecord> records = new ArrayList<>();
            for (JsonElement levelElement : levels) {
                JsonObject levelObject = levelElement.getAsJsonObject();
                int level = levelObject.get("level").getAsInt();
                JsonArray sstables = levelObject.getAsJsonArray("sstables");
                for (JsonElement sstableElement : sstables) {
                    JsonObject sstableObj = sstableElement.getAsJsonObject();
                    records.add(new ManifestRecord(
                            level,
                            sstableObj.get("fileNumber").getAsInt(),
                            Path.of(sstableObj.get("filePath").getAsString()).toAbsolutePath().toString(),
                            sstableObj.get("fileSize").getAsLong()));
                }
            }
            return records;
        }
    }

    private void waitUntilManifestMatchesMetadata() throws IOException, InterruptedException {
        TableDirectory tableDirectory = TableDirectory.getInstance();
        for (int attempt = 0; attempt < 60; attempt++) {
            Set<String> metadataPaths = snapshotMetadata(tableDirectory).stream()
                    .map(MetadataRecord::filePath)
                    .collect(Collectors.toSet());
            Set<String> manifestPaths = readManifestRecords().stream()
                    .map(ManifestRecord::filePath)
                    .collect(Collectors.toSet());
            if (metadataPaths.equals(manifestPaths)) {
                return;
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("Manifest did not converge to TableDirectory state within bounded polling");
    }

    private void triggerCompactionWithBoundedPolling(CompactionManager manager, int attempts) throws InterruptedException {
        for (int attempt = 0; attempt < attempts; attempt++) {
            manager.checkAndTriggerCompaction();
            Thread.sleep(20L);
        }
    }

    private String keyString(ByteArrayWrapper key) {
        return key == null ? "<null>" : new String(key.getData(), StandardCharsets.UTF_8);
    }

    private record ManifestRecord(int level, int fileNumber, String filePath, long fileSize) {
    }

    private record MetadataRecord(int level, int fileNumber, String filePath, long fileSize, String minKey, String maxKey) {
    }
}

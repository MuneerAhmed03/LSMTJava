package org.lsmtdb.test.utils;

import org.lsmtdb.api.KeyValueStore;
import org.lsmtdb.common.AppConstants;
import org.lsmtdb.core.compaction.CompactionManager;
import org.lsmtdb.core.compaction.LevelMetadata;
import org.lsmtdb.core.memtable.Memtable;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.SSTableWriter;
import org.lsmtdb.core.sstable.TableDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test harness for setting up and managing test database instances.
 * Provides utilities for creating isolated test environments and triggering operations.
 */
public class LSMTreeTestHarness implements AutoCloseable {
    
    private final String testName;
    private final Path testRootPath;
    private final Path testDbPath;
    private KeyValueStore kvStore;
    private TableDirectory tableDirectory;
    
    /**
     * Create a test database instance with isolated storage.
     * 
     * @param testName Unique name for this test (used for directory naming)
     * @throws IOException If database creation fails
     */
    public LSMTreeTestHarness(String testName) throws IOException {
        if (testName == null || testName.trim().isEmpty()) {
            throw new IllegalArgumentException("Test name cannot be null or empty");
        }
        
        this.testName = testName;
        this.testRootPath = Paths.get("build", "test-dbs",
                "lsmtdb_test_" + testName + "_" + System.nanoTime()).toAbsolutePath();
        this.testDbPath = testRootPath.resolve("test_db");
        initializeIsolatedState();
    }
    
    /**
     * Create and initialize a test database.
     * 
     * @param testName Unique name for this test
     * @return Configured LSMTreeTestHarness instance
     * @throws IOException If database creation fails
     */
    public static LSMTreeTestHarness createTestDatabase(String testName) throws IOException {
        return new LSMTreeTestHarness(testName);
    }
    
    /**
     * Get the KeyValueStore instance for this test.
     * Lazily initializes if not already created.
     * 
     * @return KeyValueStore instance
     * @throws IOException If initialization fails
     */
    public KeyValueStore getKeyValueStore() throws IOException {
        if (kvStore == null) {
            kvStore = KeyValueStore.getInstance(testDbPath.toString());
        }
        return kvStore;
    }
    
    /**
     * Get the test database path.
     * 
     * @return Path to the test database directory
     */
    public Path getTestDbPath() {
        return testDbPath;
    }
    
    /**
     * Get the SSTables directory path.
     * 
     * @return Path to the SSTables directory
     */
    public Path getSSTablesPath() {
        return testDbPath.resolve("sstables");
    }

    public CompactionManager createCompactionManager() {
        return new CompactionManager();
    }
    
    /**
     * Manually trigger a memtable flush to SSTable.
     * 
     * @throws IOException If flush fails
     */
    public void forceFlush() throws IOException {
        Memtable memtable = Memtable.getInstance();
        
        // Check if memtable has any data (via iterator)
        if (!memtable.iterator().hasNext()) {
            return; // Nothing to flush
        }
        
        try (SSTableWriter writer = new SSTableWriter(0)) {
            writer.write(memtable);
        }
        
        memtable.clear();
    }
    
    /**
     * Manually trigger compaction at a specific level.
     * Note: This is a simplified trigger - actual implementation depends on CompactionManager.
     * 
     * @param level Level to compact (0-based)
     * @throws IOException If compaction fails
     */
    public void forceCompaction(int level) throws IOException {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be non-negative");
        }

        LevelMetadata levelMetadata = tableDirectory.getLevelMetadata(level);
        if (levelMetadata == null) {
            throw new IllegalArgumentException("Level " + level + " does not exist");
        }

        CompactionManager manager = createCompactionManager();
        manager.checkAndTriggerCompaction();
        manager.shutdown();
    }
    
    /**
     * Wait for any async compactions to complete.
     * Polls until no compactions are running or timeout is reached.
     * 
     * @param timeoutSeconds Maximum time to wait in seconds
     * @return true if compactions completed, false if timeout
     * @throws InterruptedException If waiting is interrupted
     */
    public boolean waitForCompaction(int timeoutSeconds) throws InterruptedException {
        if (timeoutSeconds <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        
        long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        
        while (System.currentTimeMillis() < endTime) {
            // Check if any SSTables are marked as being compacted
            boolean anyCompacting = false;
            
            for (LevelMetadata level : tableDirectory.getAllLevels()) {
                for (SSTableMetadata sstable : level.getSstables()) {
                    if (sstable.isBeingCompacted()) {
                        anyCompacting = true;
                        break;
                    }
                }
                if (anyCompacting) break;
            }
            
            if (!anyCompacting) {
                return true;
            }
            
            Thread.sleep(100); // Poll every 100ms
        }
        
        return false;
    }
    
    /**
     * Get the count of SSTables at a specific level.
     * 
     * @param level Level number (0-based)
     * @return Number of SSTables at that level
     */
    public int getSSTableCount(int level) {
        LevelMetadata levelMetadata = tableDirectory.getLevelMetadata(level);
        if (levelMetadata == null) {
            return 0;
        }
        return levelMetadata.getSstables().size();
    }
    
    /**
     * Get all level metadata.
     * 
     * @return List of all LevelMetadata objects
     */
    public List<LevelMetadata> getAllLevels() {
        return tableDirectory.getAllLevels();
    }
    
    /**
     * Get current memtable entry count.
     * Note: Memtable doesn't expose direct size, so we count entries.
     * 
     * @return Number of entries in memtable
     */
    public long getMemtableSize() {
        Memtable memtable = Memtable.getInstance();
        long count = 0;
        var iterator = memtable.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }
    
    /**
     * Clean shutdown of the test database.
     * Closes any open resources.
     */
    public void shutdown() {
        // Close any open resources
        // KeyValueStore doesn't have explicit close currently
    }
    
    /**
     * Delete all test data and clean up resources.
     * 
     * @throws IOException If cleanup fails
     */
    public void cleanup() throws IOException {
        shutdown();
        resetGlobalState();
        deleteRecursively(testRootPath);
    }
    
    @Override
    public void close() throws IOException {
        cleanup();
    }
    
    /**
     * Get the test name.
     * 
     * @return Test name
     */
    public String getTestName() {
        return testName;
    }

    private void initializeIsolatedState() throws IOException {
        Files.createDirectories(testDbPath);
        Files.createDirectories(testDbPath.resolve("sstables"));
        AppConstants.setBaseDirForTests(testRootPath.toString());
        KeyValueStore.resetForTests();
        TableDirectory.resetForTests();
        Memtable.getInstance().clear();
        this.tableDirectory = TableDirectory.getInstance();
    }

    private static void resetGlobalState() {
        KeyValueStore.resetForTests();
        Memtable.getInstance().clear();
        TableDirectory.resetForTests();
        AppConstants.resetBaseDirForTests();
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

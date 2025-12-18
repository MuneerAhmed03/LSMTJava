package org.lsmtdb.core.compaction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.SSTableReader;
import org.lsmtdb.core.sstable.TableDirectory;
import org.lsmtdb.core.sstable.merger.*;


public class CompactionManager {
    private final List<LevelMetadata> levels;
    private final CompactionStrategy compactionStrategy;
    private final ExecutorService compactionExecutor;
    private final Map<Integer,Future<?>> activeCompaction;
    private final ReentrantLock compactionLock;
    private static final int MAX_CONCURRENT_COMPACTIONS = 1;
    private static final int COMPACTION_CHECK_INTERVAL_MS = 60*30;
    private final TableDirectory tableDirectory;

    public CompactionManager(){
        this.tableDirectory = TableDirectory.getInstance();
        this.levels = tableDirectory.getAllLevels();
        this.compactionStrategy = new CompactionStrategy();
        this.compactionExecutor = Executors.newFixedThreadPool(MAX_CONCURRENT_COMPACTIONS);
        this.activeCompaction = new ConcurrentHashMap<>();
        this.compactionLock = new ReentrantLock();
    }

    public void startCompactionDaemon(){
        Thread daemon = new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()){
                try{
                    checkAndTriggerCompaction();
                    Thread.sleep(COMPACTION_CHECK_INTERVAL_MS);
                } catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        daemon.setDaemon(true);
        daemon.start();
    }

    public void checkAndTriggerCompaction(){
        if(!compactionLock.tryLock()){
            return;
        }

        try{
            for(int i = 0; i< levels.size()-1 ; i++){
                LevelMetadata currentLevel = levels.get(i);
                if(compactionStrategy.shouldCompact(currentLevel)){
                    
                    LevelMetadata nextLevel = i < levels.size()-1 ? levels.get(i + 1) : null;
                    triggerCompaction(currentLevel, nextLevel);
                    break;
                }
            }
        }finally{
            compactionLock.unlock();
        }
    }

    private void triggerCompaction(LevelMetadata currentLevel, LevelMetadata nextLevel) {

        Future<?> placeholder = CompletableFuture.completedFuture(null);
        Future<?> existing = activeCompaction.putIfAbsent(nextLevel.levelNumber, placeholder);

        if (existing != null) {
            return;
        }

        try {
            List<SSTableMetadata> inputs = pickCompactionInputs(currentLevel, nextLevel);

            if (inputs.isEmpty()) {
                System.out.println(
                        "No overlapping SSTables found. Handling no-overlap case for L" +
                                currentLevel.levelNumber + " file " + currentLevel.sstables.get(0)
                );
                handleNoOverlap(currentLevel.sstables.get(0));
                activeCompaction.remove(nextLevel.levelNumber);
                return;
            }

            System.out.println("Compaction triggered at L" + currentLevel.levelNumber +
                    " → L" + nextLevel.levelNumber +
                    " for " + inputs.size() + " SSTables.");

            Future<?> future = compactionExecutor.submit(() -> {
                try {
                    performCompaction(currentLevel, nextLevel, inputs);
                } catch (Exception e) {
                    handleCompactionError(currentLevel.levelNumber, e);
                } finally {
                    activeCompaction.remove(nextLevel.levelNumber);
                }
            });
            activeCompaction.put(nextLevel.levelNumber, future);
        } catch (Exception e) {
            activeCompaction.remove(nextLevel.levelNumber);
            throw e;
        }
    }

    private List<SSTableMetadata> pickCompactionInputs(
            LevelMetadata currentLevel,
            LevelMetadata nextLevel) {

        SSTableMetadata first = currentLevel.sstables.get(0);

        if (currentLevel.levelNumber == 0) {
            return pickL0CompactionInputs(first, currentLevel.sstables, nextLevel.sstables);
        } else {
            return pickLnCompactionInputs(first, nextLevel.sstables);
        }
    }

    private List<SSTableMetadata> pickL0CompactionInputs(
            SSTableMetadata startingFile,
            List<SSTableMetadata> l0files,
            List<SSTableMetadata> nextLevelFiles) {

        List<SSTableMetadata> l0Overlaps = compactionStrategy.findl0Overlaps(startingFile, l0files);

        ByteArrayWrapper min = l0Overlaps.stream()
                .map(SSTableMetadata::getMinKey)
                .min(ByteArrayWrapper::compareTo)
                .orElse(startingFile.getMinKey());

        ByteArrayWrapper max = l0Overlaps.stream()
                .map(SSTableMetadata::getMaxKey)
                .max(ByteArrayWrapper::compareTo)
                .orElse(startingFile.getMaxKey());

        List<SSTableMetadata> nextLevelOverlaps =
                compactionStrategy.findOverlapsWithinRange(min, max, nextLevelFiles);


        List<SSTableMetadata> result = new ArrayList<>();
        result.addAll(l0Overlaps);
        result.addAll(nextLevelOverlaps);
        return result;
    }



    private List<SSTableMetadata> pickLnCompactionInputs(
            SSTableMetadata startingFile,
            List<SSTableMetadata> nextLevelFiles) {

        List<SSTableMetadata> result = compactionStrategy.findOverlaps(startingFile, nextLevelFiles);
        result.add(startingFile);
        return result;
    }

    private void performCompaction(LevelMetadata currentLevel , LevelMetadata nextLevel, List<SSTableMetadata> sstTablesToCompact) throws Exception {
        List<SSTableMetadata> newSSTables = mergeSSTables(sstTablesToCompact,nextLevel.levelNumber);

        tableDirectory.removeSSTables(sstTablesToCompact);

        if (nextLevel != null) {
            tableDirectory.addSSTables(newSSTables);
        }

        cleanupOldSSTables(sstTablesToCompact);
    }

    private List<SSTableMetadata> mergeSSTables(List<SSTableMetadata> sstablesToCompact,int nextLevel) throws Exception {
        return SSTableMerger.mergeSSTables(sstablesToCompact,nextLevel);
    }

    private void cleanupOldSSTables(List<SSTableMetadata> oldSSTables) {
        if (oldSSTables == null || oldSSTables.isEmpty()) {
            return;
        }
        for (SSTableMetadata meta : oldSSTables) {
            try{
                Path filePath = Paths.get(meta.getFilePath());
                if (!Files.exists(filePath)) {
                    continue;
                }
                File file = new File(meta.getFilePath());
                boolean deleted = file.delete();
                if (!deleted) {
                    throw new IOException("Failed to delete SSTable file: " + meta.getFilePath());
                }
            } catch (Exception e){
                System.err.println("error deleting sstable file: " + meta.getFilePath() + " - " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void handleCompactionError(int levelNumber, Exception e) {
        System.err.println("compaction error at level " + levelNumber + ": " + e.getMessage());
        e.printStackTrace();
    }

    public void shutdown() {
        compactionExecutor.shutdown();
        try {
            if (!compactionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    void handleNoOverlap(SSTableMetadata sstTable){
        SSTableIterator iterator;
        try(SSTableReader reader = new SSTableReader(sstTable.getFilePath())){
            iterator = new SSTableIterator(reader);
            int level = sstTable.getLevel();
            String newFilePath = tableDirectory.generatePath(level+1);
            int fileNumber = tableDirectory.getAndIncrementNextFileNumber();

            ByteArrayWrapper minKey = null;
            ByteArrayWrapper maxKey = null;

            try(SSTableStreamWriter writer = new SSTableStreamWriter(newFilePath)){ 
                while (iterator.hasNext()) {
                    iterator.next();

                    ByteArrayWrapper key = iterator.getCurrentKey();
                    byte[] value = iterator.getCurrentValue();
                    long timestamp = iterator.getCurrentTimestamp();

                    if(value == null) continue;

                    writer.writeEntry(key.getData(), value, timestamp);

                    if(minKey == null || key.compareTo(minKey) < 0) minKey = key;
                    if(maxKey == null || key.compareTo(maxKey) > 0) maxKey = key;
                }
                writer.finish();
            }

            if(minKey == null || maxKey == null) {
                tableDirectory.removeSSTables(level, List.of(sstTable));
                new File(sstTable.getFilePath()).delete();
                return;
            }

            File newFile = new File(newFilePath);
            SSTableMetadata newMeta = tableDirectory.allocateNewSSTable(
                level + 1, minKey, maxKey, newFile.length(), newFilePath, fileNumber
            );

            tableDirectory.removeSSTables(level, List.of(sstTable));
            tableDirectory.addSSTable(level +1, newMeta);

            new File(sstTable.getFilePath()).delete();

        }catch(IOException e){
            throw new RuntimeException("Error creating SSTableIterator for " + sstTable.getFilePath(), e);
        }
    } 
}

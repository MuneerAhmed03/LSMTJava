package org.lsmtdb.core.compaction;

import java.io.File;
import java.io.IOException;
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
        this.tableDirectory = TableDirectory.getInstance("manifest.json");
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

    private void checkAndTriggerCompaction(){
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

    private void triggerCompaction(LevelMetadata currentLevel, LevelMetadata nextLevel){
        if(activeCompaction.containsKey(nextLevel.levelNumber)){
            return;
        }

        List<SSTableMetadata> sstTablesToCompact = compactionStrategy.findOverlaps(currentLevel.sstables.get(0), nextLevel.sstables);

        if(sstTablesToCompact.isEmpty()){
            handleNoOverlap(currentLevel.sstables.get(0));
            return;
        }

        Future<?> future = compactionExecutor.submit(()->{
            try{
                performCompaction(currentLevel, nextLevel, sstTablesToCompact);
            }catch(Exception e){
                handleCompactionError(currentLevel.levelNumber, e);
            }finally{
                activeCompaction.remove(nextLevel.levelNumber);
            }
        });
        activeCompaction.put(nextLevel.levelNumber, future);
    }

    private void performCompaction(LevelMetadata currentLevel , LevelMetadata nextLevel, List<SSTableMetadata> sstTablesToCompact) throws IOException {
        List<SSTableMetadata> newSSTables = mergeSSTables(sstTablesToCompact);

        currentLevel.sstables.removeAll(sstTablesToCompact);
        currentLevel.totalSize -= sstTablesToCompact.stream().mapToLong(SSTableMetadata::getFileSize).sum();

        if (nextLevel != null) {
            nextLevel.sstables.addAll(newSSTables);
            nextLevel.totalSize += newSSTables.stream()
                .mapToLong(SSTableMetadata::getFileSize)
                .sum();
        }

        cleanupOldSSTables(sstTablesToCompact);
    }

    private List<SSTableMetadata> mergeSSTables(List<SSTableMetadata> sstablesToCompact) throws IOException {
        return SSTableMerger.mergeSSTables(sstablesToCompact);
    }

    private void cleanupOldSSTables(List<SSTableMetadata> oldSSTables) {
        for (SSTableMetadata meta : oldSSTables) {
            File file = new File(meta.getFilePath());
            boolean deleted = file.delete();
            if (!deleted) {
                System.err.println("failed to delete sstable file: " + meta.getFilePath());
            }
            tableDirectory.removeSSTables(meta.getLevel(), List.of(meta));
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

    public void addSSTableToLevel(SSTableMetadata sstable, int level) {
        if (level >= 0 && level < levels.size()) {
            LevelMetadata levelMetadata = levels.get(level);
            levelMetadata.sstables.add(sstable);
            levelMetadata.totalSize += sstable.getFileSize();
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
                level, minKey, maxKey, newFile.length(), newFilePath, fileNumber
            );

            tableDirectory.removeSSTables(level, List.of(sstTable));
            tableDirectory.addSSTable(level +1, sstTable);

            new File(sstTable.getFilePath()).delete();

        }catch(IOException e){
            throw new RuntimeException("Error creating SSTableIterator for " + sstTable.getFilePath(), e);
        }
    } 
}

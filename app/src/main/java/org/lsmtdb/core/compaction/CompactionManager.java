package org.lsmtdb.core.compaction;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.merger.SSTableMerger;

public class CompactionManager {
    private final List<LevelMetadata> levels;
    private final CompactionStrategy compactionStrategy;
    private final ExecutorService compactionExecutor;
    private final Map<Integer,Future<?>> activeCompaction;
    private final ReentrantLock compactionLock;
    private static final int MAX_CONCURRENT_COMPACTIONS = 1;
    private static final int COMPACTION_CHECK_INTERVAL_MS = 1;

    public CompactionManager(){
        this.levels = new ArrayList<>();
        this.compactionStrategy = new CompactionStrategy();
        this.compactionExecutor = Executors.newFixedThreadPool(MAX_CONCURRENT_COMPACTIONS);
        this.activeCompaction = new ConcurrentHashMap<>();
        this.compactionLock = new ReentrantLock();
        initializeLevels();
    }

    private void initializeLevels(){
        long levelSize = 8 * 4 * 1024 * 1024;

        levels.add(new LevelMetadata(0, new ArrayList<>(), 0, levelSize));

        for(int i = 1; i<7;i++){
            levelSize *= 10;
            levels.add(new LevelMetadata(i, new ArrayList<>(),0 , levelSize));
        }
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

        List<SSTableMetadata> sstTablesToCompact = compactionStrategy.findOverlaps(currentLevel.sstables.get(0), currentLevel.sstables);

        if(sstTablesToCompact.isEmpty()){
            return;
        }

        Future<?> future = compactionExecutor.submit(()->{
            try{
                performCompaction(currentLevel, nextLevel, sstTablesToCompact);
            }catch(Exception e){
                handleCompactionError(currentLevel.levelNumber, e);
            }finally{
                activeCompaction.remove(currentLevel.levelNumber);
            }
        });
        activeCompaction.put(currentLevel.levelNumber, future);
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
        // delete old sstable files
        // update manifest
        // this is a placeholder - actual implementation needed
    }

    private void handleCompactionError(int levelNumber, Exception e) {
        // log error
        // potentially retry compaction
        // this is a placeholder - actual implementation needed
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
}

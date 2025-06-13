package org.lsmtdb.core.compaction;

import java.util.ArrayList;
import java.util.List;
import org.lsmtdb.core.sstable.SSTableMetadata;


public class LevelMetadata {
    int levelNumber;
    List<SSTableMetadata> sstables;
    long totalSize;
    long maxSize;

    public LevelMetadata (int levelNumber, List<SSTableMetadata> sstables, long totalSize, long maxSize){
        this.levelNumber = levelNumber;
        this.sstables = sstables;
        this.totalSize = totalSize;
        this.maxSize = maxSize; 
    }
    public int getLevelNumber() {
        return levelNumber;
    }

    public void setLevelNumber(int levelNumber) {
        this.levelNumber = levelNumber;
    }

    public List<SSTableMetadata> getSstables() {
        return sstables;
    }

    public void setSstables(List<SSTableMetadata> sstables) {
        this.sstables = sstables;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }
    public LevelMetadata (int levelNumber){
        this.levelNumber = levelNumber;
        this.sstables = new ArrayList<>();
        this.totalSize = 0;
        this.maxSize = defaultMaxSize(levelNumber); 
    }

    

    private long defaultMaxSize(int level){
        long base = 8 * 4 * 1024 * 1024;
        for(int i =0; i< level;i++) base*=10;
        return base;
    }
}

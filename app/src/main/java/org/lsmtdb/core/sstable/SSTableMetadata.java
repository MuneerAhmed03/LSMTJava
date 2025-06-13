package org.lsmtdb.core.sstable;

import org.lsmtdb.common.ByteArrayWrapper;

public class SSTableMetadata {
    int fileNumber;
    private final String filePath;
    private final ByteArrayWrapper minKey;
    private final ByteArrayWrapper maxKey;
    private long fileSize;
    private boolean beingCompacted;
    private final int level;

    public SSTableMetadata(int fileNumber, String filePath, ByteArrayWrapper minKey, ByteArrayWrapper maxKey, long fileSize, boolean beingCompacted, int level) {
        this.filePath = filePath;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.fileSize = fileSize;
        this.beingCompacted = beingCompacted;
        this.level = level;
    }
    
    public int getFileNumber(){
        return fileNumber;
    }

    public String getFilePath() {
        return filePath;
    }

    public ByteArrayWrapper getMinKey() {
        return minKey;
    }

    public ByteArrayWrapper getMaxKey() {
        return maxKey;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize){
        this.fileSize = fileSize;
    }

    public boolean isBeingCompacted() {
        return beingCompacted;
    }

    public void setBeingCompacted(boolean beingCompacted) {
        this.beingCompacted = beingCompacted;
    }

    public int getLevel() {
        return level;
    }
}

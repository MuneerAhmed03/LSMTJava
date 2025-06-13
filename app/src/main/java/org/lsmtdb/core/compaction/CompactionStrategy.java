package org.lsmtdb.core.compaction;

import java.util.List;
import org.lsmtdb.core.sstable.SSTableMetadata;
import java.util.stream.Collectors;

public class CompactionStrategy {
    boolean shouldCompact(LevelMetadata level){
        return level.totalSize > level.maxSize;
    }

    List<SSTableMetadata> findOverlaps(SSTableMetadata target, List<SSTableMetadata> candidates){
        return candidates.stream()
            .filter(candidate -> candidate.getMinKey().compareTo(target.getMaxKey()) <= 0 && candidate.getMaxKey().compareTo(target.getMinKey()) >= 0)
            .collect(Collectors.toList());
    }
}

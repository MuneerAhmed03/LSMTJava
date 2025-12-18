package org.lsmtdb.core.compaction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.SSTableMetadata;

import java.util.Set;
import java.util.stream.Collectors;

public class CompactionStrategy {
    boolean shouldCompact(LevelMetadata level) {
        return level.totalSize > level.maxSize;
    }


    boolean rangeOverlaps(ByteArrayWrapper minKey, ByteArrayWrapper maxKey, SSTableMetadata sstTable) {
        return sstTable.getMinKey().compareTo(maxKey) <= 0 && sstTable.getMaxKey().compareTo(minKey) >= 0;
    }

    boolean overlaps(SSTableMetadata a, SSTableMetadata b) {
        return a.getMinKey().compareTo(b.getMaxKey()) <= 0 &&
                a.getMaxKey().compareTo(b.getMinKey()) >= 0;
    }


    List<SSTableMetadata> findl0Overlaps(SSTableMetadata target, List<SSTableMetadata> candidates) {

        ByteArrayWrapper minKey = target.getMinKey();
        ByteArrayWrapper maxKey = target.getMaxKey();

        Set<SSTableMetadata> result = new HashSet<>();

        boolean changed = false;

        do {
            changed = false;

            for (SSTableMetadata file : candidates) {
                if (result.contains(file)) continue;

                if (rangeOverlaps(minKey, maxKey, file)) {
                    result.add(file);

                    if (file.getMinKey().compareTo(minKey) <= 0)
                        minKey = file.getMinKey();
                    if (file.getMaxKey().compareTo(maxKey) >= 0)
                        maxKey = file.getMaxKey();

                    changed = true;
                }
            }
        } while (changed);

        return new ArrayList<>(result);
    }

    List<SSTableMetadata> findOverlaps(
            SSTableMetadata target,
            List<SSTableMetadata> candidates) {

        return candidates.stream()
                .filter(c -> overlaps(target, c))
                .collect(Collectors.toList());
    }

    List<SSTableMetadata> findOverlapsWithinRange(ByteArrayWrapper minKey, ByteArrayWrapper maxKey, List<SSTableMetadata> candidates){
        return  candidates.stream()
                .filter(c->rangeOverlaps(minKey,maxKey,c))
                .collect(Collectors.toList());
    }
}

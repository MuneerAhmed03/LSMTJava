package org.lsmtdb.core.sstable.merger;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import org.lsmtdb.core.sstable.SSTableMetadata;
import java.util.ArrayList;
import org.lsmtdb.core.sstable.SSTableReader;
import org.lsmtdb.common.ByteArrayWrapper;
import java.io.File;
import org.lsmtdb.core.sstable.TableDirectory;

public class SSTableMerger {
    List<SSTableMetadata> tablesToMerge;

    public static List<SSTableMetadata> mergeSSTables(List<SSTableMetadata> sstablesToCompact) throws IOException {
        //prepare iterators for each sstable
        PriorityQueue<SSTableIterator> heap = new PriorityQueue<>();
        ByteArrayWrapper minKey = null;
        ByteArrayWrapper maxKey = null;
        
        for(SSTableMetadata metadata : sstablesToCompact){
            try{
                SSTableReader reader = new SSTableReader(metadata.getFilePath());
                SSTableIterator iterator = new SSTableIterator(reader);
                heap.add(iterator);
            }catch(IOException e){
                throw new RuntimeException("Error creating SSTableIterator for " + metadata.getFilePath(), e);
            }
        }

        int nextLevel = sstablesToCompact.get(0).getLevel() + 1;

        TableDirectory tableDir = TableDirectory.getInstance();
        String mergedFilePath = tableDir.generatePath(nextLevel);
        int fileNumber = tableDir.getAndIncrementNextFileNumber();
        String tempMergedFilePath = mergedFilePath + ".tmp";
        System.out.println("[merger] creating temp merged file: " + tempMergedFilePath);
        java.nio.file.Path tempPath = java.nio.file.Paths.get(tempMergedFilePath);
        java.nio.file.Path finalPath = java.nio.file.Paths.get(mergedFilePath);
        
        try (SSTableStreamWriter writer = new SSTableStreamWriter(tempMergedFilePath)) {

            while(!heap.isEmpty()){
                SSTableIterator it = heap.poll();

                ByteArrayWrapper key = it.getCurrentKey();
                byte[] value = it.getCurrentValue();
                long timestamp = it.getCurrentTimestamp();

                while(!heap.isEmpty() && heap.peek().getCurrentKey().equals(key)){
                    SSTableIterator next = heap.poll();

                    if(next.getCurrentTimestamp() > timestamp){
                        value = next.getCurrentValue();
                        timestamp = next.getCurrentTimestamp();
                    }

                    if(next.hasNext()){
                        heap.add(next);
                    }
                }

                if(value != null){
                    writer.writeEntry(key.getData(), value, timestamp);
                    
                    if (minKey == null || key.compareTo(minKey) < 0) {
                        minKey = key;
                    }
                    if (maxKey == null || key.compareTo(maxKey) > 0) {
                        maxKey = key;
                    }
                }

                if(it.hasNext()){
                    heap.add(it);
                }
            }
            
            if(minKey == null || maxKey == null) {
                return new ArrayList<>();
            }

            writer.finish();
            System.out.println("[merger] finished writing temp merged file: " + tempMergedFilePath);
        }
        // atomic rename
        java.nio.file.Files.move(tempPath, finalPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        System.out.println("[merger] atomically moved temp file to final merged file: " + mergedFilePath);
        File mergedFile = new File(mergedFilePath);
        SSTableMetadata mergedMetadata = tableDir.allocateNewSSTable(
            nextLevel,
            minKey,
            maxKey,
            mergedFile.length(),
            mergedFilePath,
            fileNumber
        );
        System.out.println("[merger] created SSTableMetadata for merged file: " + mergedFilePath + ", size=" + mergedFile.length());
        List<SSTableMetadata> result = new ArrayList<>();
        result.add(mergedMetadata);
        return result;
    }
}

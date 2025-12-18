package org.lsmtdb.core.sstable.merger;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.merger.SSTableIterator;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.SSTableReader;
import org.lsmtdb.core.sstable.TableDirectory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SSTableMerger {

    public static List<SSTableMetadata> mergeSSTables(List<SSTableMetadata> sstablesToCompact,int nextLevel) throws Exception {

        List<SSTableIterator> iterators = sstablesToCompact.stream()
                .map(m -> {
                    try {
                        System.out.println("Merging SSTable: " + m.getFilePath());
                        SSTableReader r = new SSTableReader(m.getFilePath());
                        SSTableIterator it = new SSTableIterator(r);
                        return it.isValid() ? it : closeAndReturnNull(it, r);
                    } catch (Exception e) {
                        System.out.println("Error opening SSTable for merging: " + m.getFilePath());
                        throw new RuntimeException(e);
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (iterators.isEmpty()) return Collections.emptyList();

        PriorityQueue<SSTableIterator> heap = new PriorityQueue<>(iterators);


        TableDirectory tableDir = TableDirectory.getInstance();
        long maxOutputFileSize = tableDir.getMaxSSTableSizeForLevel(nextLevel);

        SSTableOutputFile outputFile = SSTableOutputFile.open(nextLevel, tableDir);
        List<SSTableMetadata> outputFiles = new ArrayList<>();

        try {
            while (!heap.isEmpty()) {

                SSTableIterator it = heap.poll();

                ByteArrayWrapper key = it.getCurrentKey();
                byte[] value = it.getCurrentValue();
                long timestamp = it.getCurrentTimestamp();

                while (!heap.isEmpty() && heap.peek().getCurrentKey().equals(key)) {
                    SSTableIterator nxt = heap.poll();

                    if (nxt.getCurrentTimestamp() > timestamp) {
                        value = nxt.getCurrentValue();
                        timestamp = nxt.getCurrentTimestamp();
                    }

                    if (nxt.hasNext()) {
                        nxt.next();
                        heap.add(nxt);
                    }
                }

                if (value != null) {
                    long sz = entrySize(key, value);
                    if (outputFile.size() + sz > maxOutputFileSize) {
                        outputFile.finish();
                        outputFiles.add(outputFile.toMetadata(nextLevel));
                        outputFile = SSTableOutputFile.open(nextLevel, tableDir);
                    }
                    outputFile.writeEntry(key, value, timestamp, sz);
                }

                if (it.hasNext()) {
                    it.next();
                    heap.add(it);
                }
            }

            outputFile.finish();
            outputFiles.add(outputFile.toMetadata(nextLevel));

        } finally {
            closeAll(iterators);
            outputFile.close();
        }

        return outputFiles;
    }

    private static SSTableIterator closeAndReturnNull(SSTableIterator it, SSTableReader r) {
        try { it.close(); r.close(); } catch (Exception ignored) {}
        return null;
    }

    private static void closeAll(List<SSTableIterator> iterators) {
        iterators.forEach(it -> {
            try { it.close(); } catch (Exception ignored) {}
        });
    }

    public static long entrySize(ByteArrayWrapper key, byte[] value) {
        int keySize = key.getData().length;
        int valueSize = (value == null) ? 0 : value.length;
        return Integer.BYTES + keySize + Integer.BYTES + valueSize + Long.BYTES;
    }
}

package org.lsmtdb.core.sstable.util;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;
import org.lsmtdb.common.ByteArrayWrapper;

public class SSTableIndexUtils {
    public interface IndexEntry {
        byte[] getKey();
        long getOffset();
    }

    public static void writeIndex(ByteBuffer buffer, List<? extends IndexEntry> index) {
        int indexSize = Integer.BYTES;
        for (IndexEntry idx : index) {
            indexSize += Integer.BYTES + idx.getKey().length + Long.BYTES;
        }
        buffer.putInt(indexSize);
        for (IndexEntry idx : index) {
            buffer.putInt(idx.getKey().length);
            buffer.put(idx.getKey());
            buffer.putLong(idx.getOffset());
        }
    }

    public static TreeMap<ByteArrayWrapper, Long> readIndex(ByteBuffer buffer, int indexSize) {
        TreeMap<ByteArrayWrapper, Long> indexMap = new TreeMap<>();
        buffer.getInt(); 
        while (buffer.hasRemaining()) {
            int keyLength = buffer.getInt();
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(keyBytes);
            long offset = buffer.getLong();
            indexMap.put(keyWrapper, offset);
        }
        return indexMap;
    }

} 
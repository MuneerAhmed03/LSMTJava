package org.lsmtdb.core.sstable.util;

import java.nio.ByteBuffer;

public class SSTableFooterUtils {
    public static void writeFooter(ByteBuffer buffer, long indexOffset, long dataOffset) {
        buffer.putLong(indexOffset);
        buffer.putLong(dataOffset);
        buffer.putLong(SSTableConstants.FOOTER_MAGIC);
    }

    public static FooterData readFooter(ByteBuffer buffer) {
        long indexOffset = buffer.getLong();
        long dataOffset = buffer.getLong();
        long magic = buffer.getLong();
        if (magic != SSTableConstants.FOOTER_MAGIC) {
            throw new IllegalArgumentException("invalid sstable file: footer magic mismatch");
        }
        return new FooterData(indexOffset, dataOffset);
    }

    public static class FooterData {
        public final long indexOffset;
        public final long dataOffset;
        public FooterData(long indexOffset, long dataOffset) {
            this.indexOffset = indexOffset;
            this.dataOffset = dataOffset;
        }
    }
} 
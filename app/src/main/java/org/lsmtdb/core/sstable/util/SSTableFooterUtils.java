package org.lsmtdb.core.sstable.util;

import java.nio.ByteBuffer;

public class SSTableFooterUtils {
    public static void writeFooter(ByteBuffer buffer, long indexOffset, long dataOffset) {
        System.out.println("writing footer: indexOffset=" + indexOffset + ", dataOffset=" + dataOffset + ", magic=" + SSTableConstants.FOOTER_MAGIC);
        int posBefore = buffer.position();
        buffer.putLong(indexOffset);
        buffer.putLong(dataOffset);
        buffer.putLong(SSTableConstants.FOOTER_MAGIC);
        int posAfter = buffer.position();
        byte[] footerBytes = new byte[posAfter - posBefore];
        int oldPos = buffer.position();
        buffer.position(posBefore);
        buffer.get(footerBytes);
        buffer.position(oldPos);
        System.out.println("footer bytes written: " + java.util.Arrays.toString(footerBytes));
    }

    public static FooterData readFooter(ByteBuffer buffer) {
        int posBefore = buffer.position();
        long indexOffset = buffer.getLong();
        long dataOffset = buffer.getLong();
        long magic = buffer.getLong();
        int posAfter = buffer.position();
        byte[] footerBytes = new byte[posAfter - posBefore];
        buffer.position(posBefore);
        buffer.get(footerBytes);
        buffer.position(posAfter);
        System.out.println("reading footer: indexOffset=" + indexOffset + ", dataOffset=" + dataOffset + ", magic=" + magic);
        System.out.println("footer bytes read: " + java.util.Arrays.toString(footerBytes));
        if (magic != SSTableConstants.FOOTER_MAGIC) {
            throw new IllegalArgumentException("invalid sstable file: footer magic mismatch " + magic + " != " + SSTableConstants.FOOTER_MAGIC);
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
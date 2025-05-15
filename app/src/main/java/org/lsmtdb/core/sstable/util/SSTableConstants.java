package org.lsmtdb.core.sstable.util;

public class SSTableConstants {
    public static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES;
    public static final int FOOTER_SIZE = Long.BYTES * 3;
    public static final long FOOTER_MAGIC = 0xFACEDBEECAFEBEEFL;
} 
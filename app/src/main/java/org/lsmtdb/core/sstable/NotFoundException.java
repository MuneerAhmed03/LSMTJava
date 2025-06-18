package org.lsmtdb.core.sstable;

public class NotFoundException extends RuntimeException {
    public NotFoundException(String message) {
        super(message);
    }
} 
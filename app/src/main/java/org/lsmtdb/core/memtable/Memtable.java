package org.lsmtdb.core.memtable;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;

public class Memtable {

    private static volatile Memtable instance;
    private static final long DEFAULT_THRESHOLD_SIZE = 8 * 1024 * 1024;
    private static volatile Long flushThresholdSizeOverrideForTests;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private TreeMap<ByteArrayWrapper, Value> store = new TreeMap<>();
    private final AtomicLong size = new AtomicLong(0);

    private Memtable(){}

    public static Memtable getInstance() {
        if (instance == null) {
            instance = new Memtable();
        }
        return instance;
    }

    public void put(ByteArrayWrapper key, Value valueObj) {
        if (key == null) {
            throw new IllegalArgumentException("Key cant be null");
        }

        lock.writeLock().lock();
        try {
            Value oldValue = this.store.get(key);
            if (oldValue != null) {
                size.addAndGet(-oldValue.getSize());
            }
            store.put(key, valueObj);
            size.addAndGet(key.getData().length + valueObj.getSize());

        } finally {
            lock.writeLock().unlock();
        }

    }

    public byte[] get(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cant be null");
        }

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

        lock.readLock().lock();
        try {
            Value valueObj = store.get(keyWrapper);
            if (valueObj == null || valueObj.isDeleted()) {
                return null;
            }
            return valueObj.getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void remove(byte[] key, long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException("Key cant be null");
        }

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

        lock.writeLock().lock();
        try {
            Value valueObj = store.get(keyWrapper);
            if (valueObj != null) {
                size.addAndGet(-valueObj.getSize());
            }

            Value tombstone = new Value(null, timestamp, true);
            store.put(keyWrapper, tombstone);
            size.addAndGet(key.length); 
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean shouldFlush() {
        return size.get() >= getFlushThresholdSizeBytes();
    }

    public ByteArrayWrapper minKey(){
        return store.firstKey();
    }

    public ByteArrayWrapper maxKey(){
        return store.lastKey();
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            store.clear();
            size.set(0);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Iterator<Map.Entry<ByteArrayWrapper,Value>> iterator () {
        lock.readLock().lock();
        try{
            TreeMap<ByteArrayWrapper,Value> copy = new TreeMap<>(store);
            return copy.entrySet().iterator();
        }finally{
            lock.readLock().unlock();
        }
    }

    static long getDefaultFlushThresholdSizeBytes() {
        return DEFAULT_THRESHOLD_SIZE;
    }

    static long getFlushThresholdSizeBytes() {
        Long override = flushThresholdSizeOverrideForTests;
        return override != null ? override : DEFAULT_THRESHOLD_SIZE;
    }

    static void setFlushThresholdSizeForTests(long thresholdSize) {
        if (thresholdSize <= 0) {
            throw new IllegalArgumentException("Flush threshold must be positive");
        }
        flushThresholdSizeOverrideForTests = thresholdSize;
    }

    static void resetFlushThresholdSizeForTests() {
        flushThresholdSizeOverrideForTests = null;
    }
}

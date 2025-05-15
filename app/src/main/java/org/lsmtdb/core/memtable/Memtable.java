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
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private TreeMap<ByteArrayWrapper, Value> store = new TreeMap<>();
    private final AtomicLong size = new AtomicLong(0);
    private final long THRESHOLD_SIZE = 8 * 1024 * 1024;

    public Memtable getInstance() {
        if (instance == null) {
            instance = new Memtable();
        }
        return instance;
    }

    public void put(byte[] key, byte[] value, long timestamp) {
        if (key == null) {
            throw new IllegalArgumentException("Key cant be null");
        }

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        Value valueObj = new Value(value, timestamp, false);
        lock.writeLock().lock();
        try {
            Value oldValue = this.store.get(keyWrapper);
            if (oldValue != null) {
                size.addAndGet(-oldValue.getSize());
            }
            store.put(keyWrapper, valueObj);
            size.addAndGet(key.length + valueObj.getSize());
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
        return size.get() >= THRESHOLD_SIZE;
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
}
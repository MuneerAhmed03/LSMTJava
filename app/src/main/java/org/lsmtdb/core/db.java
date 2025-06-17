package org.lsmtdb.core;

import org.lsmtdb.core.sstable.*;
import org.lsmtdb.core.wal.*;
import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.IOException;

public class db {
    private final WALWriter walWriter;
    private final WALReader walReader;
    private final Memtable memTable;
    private final SSTableReader sstableReader;
    private final SSTableWriter sstableWriter;
    private static final String SSTABLE_FILE = "data.sst";

    public db(String dbPath) throws IOException {
        // create necessary directories
        Path path = Paths.get(dbPath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        
        // create sstable directory
        Path sstablePath = Paths.get(dbPath + "/sstable");
        if (!Files.exists(sstablePath)) {
            Files.createDirectories(sstablePath);
        }

        this.walWriter = new WALWriter(Paths.get(dbPath + "/wal.log"), 1000);
        this.walReader = new WALReader(Paths.get(dbPath + "/wal.log"));
        this.memTable = new Memtable();
        String sstableFilePath = dbPath + "/sstable/" + SSTABLE_FILE;
        this.sstableReader = new SSTableReader(sstableFilePath);
        this.sstableWriter = new SSTableWriter(0);
    }

    public void put(String key, Object value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        Value valueObj = new Value(value.toString().getBytes(StandardCharsets.UTF_8), System.currentTimeMillis(), false);
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(keyBytes);
        walWriter.append(new WalEntry(keyWrapper, valueObj));
        memTable.put(keyWrapper, valueObj);
        if(memTable.shouldFlush()){
            sstableWriter.write(memTable);
            memTable.clear();
            walWriter.clear();
            System.out.println("flushed memtable to sstable");
        }
        
    }

    public byte[] get(byte[] key) throws IOException {
        if (memTable.get(key) != null) {    
            return memTable.get(key);
        } else {
            return sstableReader.get(key);
        }
    }

    public void delete(String key) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        Value valueObj = new Value(null, System.currentTimeMillis(), true);
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(keyBytes);
        walWriter.append(new WalEntry(keyWrapper, valueObj));
        memTable.put(keyWrapper, valueObj);
        if(memTable.shouldFlush()){
            sstableWriter.write(memTable);
            memTable.clear();
            walWriter.clear();
            System.out.println("flushed memtable to sstable");
        }
    }
}

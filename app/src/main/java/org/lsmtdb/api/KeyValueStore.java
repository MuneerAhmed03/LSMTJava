package org.lsmtdb.api;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;
import org.lsmtdb.core.memtable.Memtable;

import org.lsmtdb.core.sstable.SSTableSearch;
import org.lsmtdb.core.sstable.SSTableWriter;
import org.lsmtdb.core.wal.WALWriter;
import org.lsmtdb.core.wal.WalEntry;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.lsmtdb.core.compaction.CompactionManager;

public class KeyValueStore implements IKeyValueStore {

    private final WALWriter walWriter;
    // private final WALReader walReader;
    private final Memtable memTable;
    private final SSTableWriter sstableWriter;
    private SSTableSearch ssTableSearch;
    private static KeyValueStore keyValueStore;
    private final CompactionManager compactionManager;
    

    private KeyValueStore(String dbPath) throws IOException{
        Path path = Paths.get(dbPath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        
        Path sstablePath = Paths.get(dbPath + "/sstables");
        if (!Files.exists(sstablePath)) {
            Files.createDirectories(sstablePath);
        }

        this.walWriter = new WALWriter(Paths.get(dbPath + "/wal.log"), 1000);
        // this.walReader = new WALReader(Paths.get(dbPath + "/wal.log"));
        this.memTable = Memtable.getInstance();
        // String sstableFilePath = dbPath + "/sstable/" + SSTABLE_FILE;
        // this.sstableReader = new SSTableReader(sstableFilePath);
        this.sstableWriter = new SSTableWriter(0);
        this.ssTableSearch =  new SSTableSearch();
        this.compactionManager = new CompactionManager();
        this.compactionManager.startCompactionDaemon();
    }

    public static KeyValueStore getInstance(String dbPath) throws IOException{
        if(keyValueStore == null){
            keyValueStore = new KeyValueStore(dbPath);
        }
        return keyValueStore;
    }


    @Override
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
            compactionManager.checkAndTriggerCompaction();
        }
    }

    @Override
    public String get(String key) throws IOException {
        byte [] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        if (memTable.get(keyBytes) != null) {    
            byte[] value =  memTable.get(keyBytes);
            return new String(value, StandardCharsets.UTF_8);
        } else {
            ByteArrayWrapper kArrayWrapper =  new ByteArrayWrapper(keyBytes);
            byte[] value = ssTableSearch.search(kArrayWrapper);
            return new String(value, StandardCharsets.UTF_8);
        }
    }

    @Override
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
            compactionManager.checkAndTriggerCompaction();
        }
    }
    
}

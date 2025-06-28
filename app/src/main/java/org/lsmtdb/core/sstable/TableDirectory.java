package org.lsmtdb.core.sstable;

import java.io.*;
import java.text.CollationElementIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.AppConstants;
import org.lsmtdb.core.compaction.LevelMetadata;


import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TableDirectory {
    private static TableDirectory instance;
    private final File manifestFile;      
    private final Map<Integer, LevelMetadata> levels = new HashMap<>();
    private int nextFileNumber = 1;
    private final ExecutorService manifestExecutor = Executors.newSingleThreadExecutor();
    private Future<?> lastSaveOperation;

    private TableDirectory(String manifestFile){
        this.manifestFile = new File(AppConstants.BASE_DB_PATH,manifestFile);
        loadManifest();
    }

    public static synchronized TableDirectory getInstance() {
        if (instance == null) {
            instance = new TableDirectory(AppConstants.MANIFEST_PATH);
        }
        return instance;
    }

    public List<SSTableMetadata> getSSTablesAtLevel(int level){
        return levels.getOrDefault(level, new LevelMetadata(level)).getSstables();
    }

    public LevelMetadata getLevelMetadata(int level){
        return levels.get(level);
    }

    public List<LevelMetadata> getAllLevels(){
        return new ArrayList<>(levels.values());
    }

    public void addSSTable(int level, SSTableMetadata sstable){
        LevelMetadata meta = levels.computeIfAbsent(level, l->new LevelMetadata(level));

        List<SSTableMetadata> temp = meta.getSstables();
        temp.add(sstable);
        Collections.sort(temp,Comparator.comparing(SSTableMetadata::getMinKey));
        meta.setSstables(temp);
        meta.setTotalSize(meta.getTotalSize() + sstable.getFileSize());
        
        levels.put(level,meta);
        saveManifest();
    }

    public void removeSSTables(int level, List<SSTableMetadata> toRemove){
        LevelMetadata meta = levels.get(level);
        if(meta == null){
            return;
        }

        long metaSize = meta.getTotalSize();
        for(SSTableMetadata s : toRemove){
            metaSize -= s.getFileSize();
        }
        meta.setTotalSize(metaSize);
        
        List<SSTableMetadata> temp = meta.getSstables();
        temp.removeIf(s -> toRemove.stream().anyMatch(r -> r.getFileNumber() == s.getFileNumber()));
        meta.setSstables(temp);
        
        saveManifest();
    }

    public String generatePath(int level){
        return AppConstants.BASE_DB_PATH + "/" + "sstables/L" + level + "/" + nextFileNumber +".sst";
    }

    public int getAndIncrementNextFileNumber(){
        return nextFileNumber++;
    }

    public SSTableMetadata allocateNewSSTable(int level, ByteArrayWrapper minkey, ByteArrayWrapper maxKey, long fileSize, String path, int fileNumber){
        SSTableMetadata meta = new SSTableMetadata(fileNumber, path, minkey, maxKey, fileSize, false, level);
        return meta;
    }

    private void saveManifest(){
        if (lastSaveOperation != null && !lastSaveOperation.isDone()) {
            lastSaveOperation.cancel(true);
        }
        lastSaveOperation = manifestExecutor.submit(() -> {
            File tempFile = new File(manifestFile.getAbsolutePath() + ".tmp");
            try {
                try (FileWriter writer = new FileWriter(tempFile)) {
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();

                    JsonObject manifest = new JsonObject();
                    manifest.addProperty("nextFileNumber", nextFileNumber);

                    JsonArray levelsJson = new JsonArray();
                    for(LevelMetadata meta : levels.values()){
                        JsonObject levelObj = new JsonObject();
                        levelObj.addProperty("level", meta.getLevelNumber());
                        levelObj.addProperty("maxSize", meta.getMaxSize());
                        levelObj.addProperty("totalSize", meta.getTotalSize());
                        levelObj.add("sstables", gson.toJsonTree(meta.getSstables()));
                        levelsJson.add(levelObj);
                    }

                    manifest.add("levels",levelsJson);
                    gson.toJson(manifest,writer);
                }

                try (FileOutputStream fos = new FileOutputStream(tempFile, true)) {
                    fos.getFD().sync();
                }
                if (!tempFile.renameTo(manifestFile)) {
                    throw new IOException("failed to atomically replace manifest");
                }
            } catch (IOException e) {
                System.err.println("error saving manifest: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    public void shutdown() {
        manifestExecutor.shutdown();
    }

    private void loadManifest(){
        if(!manifestFile.exists()){
            for(int i = 0; i < 5;i++){
                levels.put(i,new LevelMetadata(i));
            }
            System.out.println("building manifest");
            saveManifest();
        } 
        try(BufferedReader reader = new BufferedReader(new FileReader(manifestFile))){
            Gson gson = new Gson();
            JsonObject manifest = JsonParser.parseReader(reader).getAsJsonObject();

            this.nextFileNumber = manifest.get("nextFileNumber").getAsInt();

            JsonArray levelsJson = manifest.getAsJsonArray("levels");
            for(JsonElement elem : levelsJson){
                JsonObject levelObj = elem.getAsJsonObject();
                int level = levelObj.get("level").getAsInt();
                long maxSize = levelObj.get("maxSize").getAsLong();
                long totalSize = levelObj.get("totalSize").getAsLong();

                List<SSTableMetadata> sstables = gson.fromJson(
                    levelObj.get("sstables"), new TypeToken<List<SSTableMetadata>>() {}.getType());

                levels.put(level,new LevelMetadata(level,sstables,totalSize,maxSize));
            } 
        } catch (IOException e){
            throw new RuntimeException("Failed to load Manifest", e);
        }
    }



}

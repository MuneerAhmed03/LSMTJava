package org.lsmtdb.core.sstable;

import org.lsmtdb.core.compaction.LevelMetadata;
import org.lsmtdb.core.sstable.SSTableMetadata;

import java.io.IOException;
import java.util.List;
import org.lsmtdb.common.ByteArrayWrapper;;

public class SSTableSearch {
    private TableDirectory tableDirectory;
    private List<LevelMetadata> levels;

    public SSTableSearch(){
        this.tableDirectory = TableDirectory.getInstance();
    }

    public byte[] search(ByteArrayWrapper key) throws IOException{
        levels = tableDirectory.getAllLevels();

        for(LevelMetadata level : levels){

            int levelNumber = level.getLevelNumber();
            List<SSTableMetadata> levelCandidates = tableDirectory.getSSTablesAtLevel(levelNumber);

            if(levelNumber == 0){
                for(int i = 0; i<levelCandidates.size(); i++){
                    SSTableMetadata l0Candidate = levelCandidates.get(i);
                    if(checkWithinRange(l0Candidate, key)){
                       
                        try( SSTableReader ssTableReader =  new SSTableReader(l0Candidate.getFilePath());){
                            byte[] value = ssTableReader.get(key.getData());
                            return value;
                        }catch(NotFoundException e){
                            continue;
                        }
                    }
                }
            }else {
                int left = 0;
                int right = levelCandidates.size() - 1;
                while (left <= right) {
                    int mid = left + (right - left) / 2;
                    SSTableMetadata midSSTable = levelCandidates.get(mid);
                    ByteArrayWrapper minKey = midSSTable.getMinKey();
                    ByteArrayWrapper maxKey = midSSTable.getMaxKey();
                    if (key.compareTo(minKey) < 0) {
                        right = mid - 1;
                    } else if (key.compareTo(maxKey) > 0) {
                        left = mid + 1;
                    } else {
                        try( SSTableReader ssTableReader =  new SSTableReader(midSSTable.getFilePath());) {
                            byte[] value = ssTableReader.get(key.getData());
                            return value;
                        } catch (NotFoundException e) {
                            break;
                        }
                    }
                }
            }
        }
        throw new NotFoundException("key not present");
    }

    private boolean checkWithinRange(SSTableMetadata sstable, ByteArrayWrapper key){
        ByteArrayWrapper minKey = sstable.getMinKey();
        ByteArrayWrapper maxKey = sstable.getMaxKey();
        if(key.compareTo(maxKey) < 0 && key.compareTo(minKey) > 0){
            return true;
        }
        return false;
    }
}

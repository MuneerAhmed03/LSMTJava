package org.lsmtdb.core.sstable.merger;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.core.sstable.SSTableMetadata;
import org.lsmtdb.core.sstable.TableDirectory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class SSTableOutputFile {
    private final String filePath;
    private final String tempFilePath;
    private final SSTableStreamWriter writer;
    private final int fileNumber;

    private ByteArrayWrapper minKey = null;
    private ByteArrayWrapper maxKey = null;
    private long bytesWritten = 0;

    private SSTableOutputFile(String filePath, String tempFilePath, SSTableStreamWriter writer,int fileNumber) {
        this.filePath = filePath;
        this.tempFilePath = tempFilePath;
        this.writer = writer;
        this.fileNumber = fileNumber;
    }

    public static SSTableOutputFile open(int levelNumber, TableDirectory tableDir) throws Exception {
        String filePath = tableDir.generatePath(levelNumber);
        int fileNumber = tableDir.getAndIncrementNextFileNumber();
        String tempFilePath = filePath + ".tmp";
        SSTableStreamWriter writer = new SSTableStreamWriter(tempFilePath);
        return new SSTableOutputFile(filePath, tempFilePath, writer, fileNumber);
    }

    public void writeEntry(ByteArrayWrapper key, byte[] value, long timestamp,long entrySize) throws Exception {
        writer.writeEntry(key.getData(), value, timestamp);
        bytesWritten += entrySize;

        if (minKey == null || key.compareTo(minKey) < 0) {
            minKey = key;
        }
        if (maxKey == null || key.compareTo(maxKey) > 0) {
            maxKey = key;
        }
    }

    public long size() {
        return bytesWritten;
    }

    public void finish() throws Exception {
        writer.finish();
        writer.close();
        Files.move(Paths.get(tempFilePath), Paths.get(filePath), StandardCopyOption.REPLACE_EXISTING);
    }

    public SSTableMetadata toMetadata(int levelNumber) {
        TableDirectory tableDir = TableDirectory.getInstance();
        return tableDir.allocateNewSSTable(
                levelNumber,
                minKey,
                maxKey,
                size(),
                filePath,
                fileNumber
        );
    }
    public void close() throws Exception {
        writer.close();
    }

}

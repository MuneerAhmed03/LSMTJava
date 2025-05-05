package org.lsmtdb.core.sstable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;

import org.lsmtdb.core.memtable.*;;

public class SSTableWriter {
    private final FileChannel channel;
    private long currentOffset;
    private final List<IndexEntry> index = new ArrayList<>();

    static class  IndexEntry {
        byte[] key;
        long offset;

        IndexEntry(byte[] key, long offset){
            this.key = key;
            this.offset = offset;
        }
        
    }

    public SSTableWriter(String filepath) throws IOException {
        File file = new File(filepath);
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.currentOffset = 0;
    }

    public void write(Memtable memtable) throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(1024*1024);
        Iterator<Map.Entry<ByteArrayWrapper, Value>> it = memtable.iterator();

        while(it.hasNext()){
            long entryOffset = currentOffset + buffer.position();
            Map.Entry<ByteArrayWrapper,Value> entry = it.next();

            byte[] key = entry.getKey().getData();
            Value value = entry.getValue();

            int keyLength = key.length;
            int valueLength = value.isDeleted() ? 0 : value.getValue().length;
            int entrySize = Integer.BYTES + keyLength + Long.BYTES + Byte.BYTES + (value.isDeleted() ? 0 : Integer.BYTES + valueLength);

            if(buffer.remaining() < entrySize){
                buffer.flip();
                channel.write(buffer,currentOffset);
                currentOffset+=buffer.position();
                buffer.clear();
            }

            if(index.isEmpty() || index.size()%128 == 0){
                index.add(new IndexEntry(key, entryOffset));
            }

            buffer.putInt(keyLength);
            buffer.put(key);
            buffer.putLong(value.getTimestamp());
            buffer.put((byte) (value.isDeleted() ? 1 : 0));
            if(!value.isDeleted()){
                buffer.putInt(valueLength);
                buffer.put(value.getValue());
            }
        }

        buffer.flip();
        channel.write(buffer,currentOffset);
        currentOffset+=buffer.position();
        buffer.clear();

        long indexOffset = currentOffset;
        int indexSize = Integer.BYTES;
        for(IndexEntry idx : index ) {
            indexSize += Integer.BYTES + idx.key.length + Long.BYTES;
        }

        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        indexBuffer.putInt(indexSize);
        for(IndexEntry idx : index) {
            indexBuffer.putInt(idx.key.length);
            indexBuffer.put(idx.key);
            indexBuffer.putLong(idx.offset);
        }

        indexBuffer.flip();
        channel.write(indexBuffer,currentOffset);
        currentOffset+=indexBuffer.position();

        ByteBuffer footerBuffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
        footerBuffer.putLong(indexOffset);
        footerBuffer.putLong(0xFACEDBEECAFEBEEFL);
        footerBuffer.flip();
        channel.write(footerBuffer,currentOffset);

        channel.close();

    }
}

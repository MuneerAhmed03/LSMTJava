package org.lsmtdb.core.wal;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.lsmtdb.common.ByteArrayWrapper;
import org.lsmtdb.common.Value;;

public class WalEntry {
    public final ByteArrayWrapper key;
    public final Value value;

    public WalEntry(ByteArrayWrapper key,Value val){
        this.key = key;
        this.value = val;
    }

    public void serialize(ByteBuffer buffer){
        buffer.putInt(this.key.getData().length);
        buffer.putInt(this.value.isDeleted() ? -1 : this.value.getValue().length);
        buffer.putLong(this.value.getTimestamp());
        buffer.put(this.key.getData());
        if (!value.isDeleted()) {
            buffer.put(value.getValue());
        }
    }

    public static WalEntry deserialize(ByteBuffer buffer){
        if(buffer.remaining() < (Integer.BYTES + Integer.BYTES + Long.BYTES)){
            throw new BufferUnderflowException();
        }

        int keyLen = buffer.getInt();
        int valLen = buffer.getInt();
        long timestamp = buffer.getLong();

        byte[] key = new byte[keyLen];
        buffer.get(key);
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);

        byte[] val = null;
        Value valObj;
        if(valLen > 0){
            val = new byte[valLen];
            buffer.get(val);
            valObj = new Value(val, timestamp, false);
        }else{
            valObj = new Value(null, timestamp, true);
        }
        return new WalEntry(keyWrapper,valObj);
    }

    public int serializedSize() {
        int size = Integer.BYTES + Integer.BYTES + Long.BYTES + key.getData().length;
        if (!value.isDeleted()) {
            size += value.getValue().length;
        }
        return size;
    }
}

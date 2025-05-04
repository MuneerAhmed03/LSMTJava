package org.lsmtdb.core.memtable;

public class Value {
    public final byte[] value;
    public final long timestamp;
    public final boolean isDeleted;

    public Value(byte[] value, long timestamp,boolean isDeleted){
        this.value = value;
        this.timestamp = timestamp;
        this.isDeleted = isDeleted;
    }

    public byte[] getValue(){
        return this.value;
    }

    public long getTimestamp(){
        return this.timestamp;
    }

    public boolean isDeleted(){
        return this.isDeleted;
    }

    public int getSize(){
        return  (value!=null) ? value.length : 0;
    }
}

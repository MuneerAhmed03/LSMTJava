package org.lsmtdb.core.memtable;

public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
    private final byte[] data;

    public ByteArrayWrapper(byte[] data){
        this.data = data;
    }

    @Override
    public boolean equals(Object other){
        if(!(other instanceof ByteArrayWrapper)){
            return false;
        }
        return compareTo((ByteArrayWrapper) other) ==0;
    }

    @Override
    public int hashCode(){
        int result = 1;
        for( byte b : data){
            result = 31 * result + b;
        }
        return result;
    }

    @Override
    public int compareTo(ByteArrayWrapper other){
        int minLength = Math.min(data.length, other.data.length);
        for(int i = 0; i<minLength;i++){
            int cmp = Byte.compare(data[i], other.data[i]);
            if(cmp!=0){
                return cmp;
            }
        }
        return Integer.compare(data.length, other.data.length);
    }

    public byte[] getData(){
        return  this.data;
    }
}
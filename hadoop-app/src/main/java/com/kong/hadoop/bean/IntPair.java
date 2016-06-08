package com.kong.hadoop.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 二次计较bean
 * Created by kong on 2016/5/13.
 */
public class IntPair implements WritableComparable<IntPair> {
    private String first;
    private String second;

    public IntPair() {
    }

    public IntPair(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IntPair other = (IntPair) obj;
        if (first == null){
            if (other.first!=null)
                return false;
        }else if (!first.equals(other.first))
            return false;
        if (second == null){
            if (other.second!=null)
                return false;
        }else if (!second.equals(other.second))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime*result+((first == null)?0:first.hashCode());
        result = prime*result+((second == null)?0:second.hashCode());
        return result;
    }

    public int compareTo(IntPair o) {
        if (this.first.equals(o.getFirst())) {
            return this.first.compareTo(o.first);
        } else {
            if (this.second.equals(o.second))
                return this.second.compareTo(o.second);
            return 0;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.first);
        dataOutput.writeUTF(this.second);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readUTF();
    }
}

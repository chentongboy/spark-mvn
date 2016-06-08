package com.kong.hadoop.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义key
 * Created by kong on 2016/4/27.
 */
public class MemberKey implements WritableComparable<MemberKey> {
    private int keyId;
    private boolean flag;

    public MemberKey() {
    }

    public MemberKey(int keyId, boolean flag) {
        this.keyId = keyId;
        this.flag = flag;
    }

    @Override
    public int hashCode() {
        return this.keyId;
    }

    public int getKeyId() {
        return keyId;
    }

    public void setKeyId(int keyId) {
        this.keyId = keyId;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public int compareTo(MemberKey o) {
        if (this.keyId == o.keyId) {
            if (this.flag == o.flag) {
                return 0;
            }
            return this.flag ? -1 : 1;
        }
        return this.keyId > o.keyId ? 1 : -1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.keyId);
        dataOutput.writeBoolean(this.flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.keyId = dataInput.readInt();
        this.flag = dataInput.readBoolean();
    }
}

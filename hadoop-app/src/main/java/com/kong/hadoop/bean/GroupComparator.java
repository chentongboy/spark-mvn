package com.kong.hadoop.bean;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 构建group用的key
 * Created by kong on 2016/5/13.
 */
public class GroupComparator extends WritableComparator {
    public GroupComparator() {
        super(MemberKey.class);
    }

    public GroupComparator(MemberKey memberKey) {
        super(memberKey.getClass());
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MemberKey keyA = (MemberKey) a;
        MemberKey keyB = (MemberKey) b;
        if (keyA.getKeyId() == keyB.getKeyId()) {
            return 0;
        }
        return keyA.getKeyId() > keyB.getKeyId() ? 1 : -1;
    }
}

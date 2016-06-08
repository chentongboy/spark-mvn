package com.kong.hadoop.bean;

import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义二次排序的group
 * Created by kong on 2016/4/28.
 */
public class SecondaryGroup extends WritableComparator {
    public SecondaryGroup() {
        super(IntPair.class,true);
    }
}

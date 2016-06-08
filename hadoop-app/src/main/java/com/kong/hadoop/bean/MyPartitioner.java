package com.kong.hadoop.bean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by kong on 2016/5/13.
 */
public class MyPartitioner extends Partitioner<IntPair, Text> {

    @Override
    public int getPartition(IntPair key, Text value, int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

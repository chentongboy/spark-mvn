package com.kong.hadoop;

import com.kong.hadoop.bean.IntPair;
import com.kong.hadoop.bean.MyPartitioner;
import com.kong.hadoop.bean.SecondaryGroup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 二次排序
 * Created by kong on 2016/4/28.
 */
public class SecondarySort {
    public static class DataMapper extends Mapper<LongWritable, Text, IntPair, Text> {
        private IntPair intPair = new IntPair();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split(" ");
            intPair.setFirst(lines[0]);
            intPair.setSecond(lines[1]);
            context.write(intPair,value);
        }
    }

    public static class DataReducer extends Reducer<IntPair, Text, NullWritable, Text> {

        @Override
        protected void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values){
                context.write(NullWritable.get(),text);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("SecondarySort");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "SecondarySort");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(SecondaryGroup.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

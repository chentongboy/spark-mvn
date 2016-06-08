package com.kong.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 数据排序
 * Created by kong on 2016/4/27.
 */
public class SortData {
    public static class SortDataMapper extends Mapper<Object, Text, LongWritable, LongWritable> {

        private LongWritable data = new LongWritable(1);
        private LongWritable eValue = new LongWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            data.set(Long.valueOf(value.toString()));
            context.write(data, eValue);
        }
    }

    public static class SortDataReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        private LongWritable position = new LongWritable(1);

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable item : values) {
                context.write(position, key);
                position.set(position.get() + 1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Sort <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "SortData");
        job.setJarByClass(SortData.class);
        job.setMapperClass(SortDataMapper.class);
        job.setReducerClass(SortDataReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

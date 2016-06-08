package com.kong.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

/**
 * topN统计
 * Created by kong on 2016/5/12.
 */
public class TopNSorted {
    public static class TopNMapper extends Mapper<Text, Text, Text, Text> {
        int[] topN;
        int length;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            length = context.getConfiguration().getInt("topN", 5); //默认为5
            topN = new int[length + 1];
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            if (data.length == 4) {
                topN[0] = Integer.parseInt(data[2]);
                Arrays.sort(topN);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < length + 1; i++) {
                context.write(new Text(String.valueOf(topN[i])), new Text(String.valueOf(topN[i])));
            }
        }
    }

    public static class TopNReducer extends Reducer<Text, Text, Text, Text> {
        int[] topN;
        int length;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            length = context.getConfiguration().getInt("topN", 5); //默认为5
            topN = new int[length + 1];
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            topN[0] = Integer.parseInt(key.toString());
            Arrays.sort(topN);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = length; i > 0; i--) {
                context.write(new Text(String.valueOf(topN[i])), new Text(String.valueOf(topN[i])));
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("topN",3);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("22");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "TopNSorted");
        job.setJarByClass(TopNSorted.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

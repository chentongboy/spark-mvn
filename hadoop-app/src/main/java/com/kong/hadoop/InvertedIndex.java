package com.kong.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 倒排索引
 * Created by kong on 2016/5/13.
 */
public class InvertedIndex {
    public static class DataMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text number = new Text("1");
        private String fileName;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().length() > 0) {
                StringTokenizer st = new StringTokenizer(line, " ");
                while (st.hasMoreTokens()) {
                    String rKey = st.nextToken() + ":" + fileName;
                    context.write(new Text(rKey), number);
                }
            }

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            fileName = inputSplit.getPath().getName();
        }
    }

    public static class DataCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text item : values) {
                sum += Integer.valueOf(item.toString());
            }
            String[] keyArray = key.toString().split(":");

            context.write(new Text(keyArray[0]), new Text(keyArray[1] + ":" + sum));
        }
    }

    public static class DataReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer result = new StringBuffer();
            for (Text text : values) {
                result.append(text + ";");
            }
            context.write(key, new Text(result.toString()));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("22");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(DataMapper.class);
        job.setCombinerClass(DataCombiner.class);
        job.setReducerClass(DataReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

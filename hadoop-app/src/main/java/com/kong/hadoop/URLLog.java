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
 * URL流量分析
 * Created by kong on 2016/4/27.
 */
public class URLLog {
    public static class URLLogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LongWritable resultValue = new LongWritable(1);
        private Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String result = handleLine(line);

            if (result != null && result.length() > 0) {
                text.set(result);
                context.write(text, resultValue);
            }
        }

        private String handleLine(String line) {
            StringBuffer buffer = new StringBuffer();

            if (line.length() > 0) {
                if (line.contains("GET")) {
                    buffer.append(line.substring(line.indexOf("GET"), line.indexOf("HTTP/1.0")).trim());
                } else if (line.contains("POST"))
                    buffer.append(line.substring(line.indexOf("POST"), line.indexOf("HTTP/1.0")).trim());
            }
            return buffer.toString();
        }
    }

    public static class URLLogReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable totalResult;

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (LongWritable item : values) {
                total += item.get();
            }

            totalResult.set(total);
            context.write(key, totalResult);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: URLLog <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "URLLog");
        job.setJarByClass(URLLog.class);
        job.setJobName("UrlLog");
        job.setMapperClass(URLLogMapper.class);
        job.setReducerClass(URLLogReducer.class);
        job.setCombinerClass(URLLogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

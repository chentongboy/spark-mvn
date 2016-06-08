package com.kong.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * 平均值与最大最小值
 * Created by kong on 2016/5/12.
 */
public class Average {
    public static class AverageMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text rKey = new Text();
        private FloatWritable val = new FloatWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            StringTokenizer split = new StringTokenizer(data, "\n");
            while (split.hasMoreElements()) {
                StringTokenizer record = new StringTokenizer(split.nextToken());
                String name = record.nextToken();
                String score = record.nextToken();
                rKey.set(name);
                val.set(Float.parseFloat(score));
                context.write(rKey, val);
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable val = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<FloatWritable> iterator = values.iterator();

            float sum = 0;
            int count = 0;
            float min = 0;
            float max = 0;

            while (iterator.hasNext()) {
                float tmp = iterator.next().get();
                sum += tmp;
                if (count == 0) {
                    min = tmp;
                    max = tmp;
                } else {
                    min = Math.min(min, tmp);
                    max = Math.max(max, tmp);
                }
                count++;
            }

            float ave = sum / count;
            val.set(ave);
            context.write(key, val);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Average <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "SortData");
        job.setJarByClass(SortData.class);
        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

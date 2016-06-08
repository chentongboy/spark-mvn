package com.kong.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 0067011990999991950051507004888888889999999N9+00001+9999999999999999999999
 * 0067011990999991950051512004888888889999999N9+00221+9999999999999999999999
 * 0067011990999991950051518004888888889999999N9-00111+9999999999999999999999
 * 0067011990999991949032412004888888889999999N9+01111+9999999999999999999999
 * 0067011990999991950032418004888888880500001N9+00001+9999999999999999999999
 * 0067011990999991950051507004888888880500001N9+00781+9999999999999999999999
 * 元数据描述：
 * 第15-19个字符表示year，例如1950年、1949年等；
 * 第45-50个字符表示的是温度，例如-00111、+00001
 * 第50位只能是0、1、4、5、9等几个数字；
 * 补充说明：在生产环境下我们拿到的Log信息一般都有很多列，具体的列和列的组合构成了实际上不同的业务意义；
 * 通过分析气象的日志数据来具体计算出气象日志的的相关统计数据
 * Created by kong on 2016/4/27.
 */
public class TemperatureComputation {
    public static class TeperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final int MISSING = 9999;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String year = data.substring(15, 19);
            int temperature = 0;
            if (data.charAt(45) == '+') {
                temperature = Integer.parseInt(data.substring(46, 50));
            } else
                temperature = Integer.parseInt(data.substring(45, 50));

            String validDataFlag = data.substring(50, 51);

            if (temperature != MISSING && validDataFlag.matches("[01459]")) {
                context.write(new Text(year), new IntWritable(temperature));
            }
        }
    }

    public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int coldestTemperature = Integer.MAX_VALUE;
            for (IntWritable item : values) {
                coldestTemperature = Math.min(coldestTemperature, item.get());
            }
            context.write(key, new IntWritable(coldestTemperature));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2){
            System.err.print("[01459]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(TemperatureComputation.class);
        job.setJobName("TemperatureComputation");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TeperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

package com.kong.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * 用户平均时间间隔
 * Created by lenovo on 2016/6/13.
 */
public class UserAverageTime {
    public class UserAverageTimeMapper extends Mapper<Text, Text, Text, LongWritable> {
        private Text rKey = new Text();
        private LongWritable rValue = new LongWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            try {
                String[] split = data.split(",");
                String MSID = split[0];
                String startDate = split[1];
                String startTime = split[2];
                if (startTime.length() < 6) {
                    StringBuffer sb = null;
                    for (int i = 0; i < 6 - startTime.length(); i++) {
                        sb.append("0");
                    }
                    startTime = sb.toString() + startTime;
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss");
                long timestamp = sdf.parse(startDate + "-" + startTime).getTime();
                rKey.set(MSID);
                rValue.set(timestamp);
                context.write(rKey, rValue);
            } catch (Exception e) {
                System.out.println("first step");
            }
        }
    }

    public class UserAverageTimeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable rValue = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<LongWritable> iterator = values.iterator();
            long count = 0L;
            long sum = 0L;
            long temp = 0L;
            while (iterator.hasNext()) {
                if (count == 0) {
                    temp = Long.valueOf(iterator.next().toString());
                } else {
                    long next = Long.valueOf(iterator.next().toString());
                    sum += (next - temp);
                }
                count++;
            }
            float avg = sum / (count - 1);
            context.write(key,rValue);
        }
    }
}

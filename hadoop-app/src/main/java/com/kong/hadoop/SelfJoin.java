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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 自连接
 * Created by kong on 2016/5/13.
 */
public class SelfJoin {
    public static class DataMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text lKey = new Text();
        private Text lVal = new Text();
        private Text rKey = new Text();
        private Text rVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split(" ");
            lKey.set(lines[1]);
            lVal.set("1_"+lines[0]+"_" + lines[1]);
            rKey.set(lines[0]);
            rVal.set("0_"+lines[0]+"_" + lines[1]);
            context.write(lKey, lVal);
            context.write(rKey,rVal);
        }
    }
    public static class DataReducer extends Reducer<Text,Text,NullWritable,Text> {
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            List<String> grandChildList = new ArrayList<String>();
            List<String> grandParentList = new ArrayList<String>();

            while (iterator.hasNext()){
                String item = iterator.next().toString();
                String[] split = item.split("_");
                if (split[0].equals("1")){
                    grandChildList.add(split[1]);
                }else
                    grandParentList.add(split[1]);
            }

            if (grandChildList.size()>0&&grandParentList.size()>0){
                for (String grandChild : grandChildList){
                    for (String grandParent : grandParentList){
                        result.set(grandChild+"-"+grandParent);
                        context.write(NullWritable.get(),result );
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("SelfJoin");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "SelfJoin");
        job.setJarByClass(SelfJoin.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

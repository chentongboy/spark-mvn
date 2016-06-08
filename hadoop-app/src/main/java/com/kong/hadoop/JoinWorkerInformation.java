package com.kong.hadoop;

import com.kong.hadoop.bean.WorkerInfo;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Join操作
 * Created by kong on 2016/4/27.
 */
public class JoinWorkerInformation {
    public static class DataMapper extends Mapper<LongWritable, Text, LongWritable, WorkerInfo> {
        private LongWritable rKey = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            String[] data = input.split("\t");

            if (data.length <= 3) {
                WorkerInfo department = new WorkerInfo();
                department.setDepartmentNo(data[0]);
                department.setDepartmentName(data[1]);
                department.setFlag(0);
                rKey.set(Long.valueOf(department.getDepartmentNo()));
                context.write(rKey, department);
            } else {
                WorkerInfo worker = new WorkerInfo();
                worker.setWorkerNo(data[0]);
                worker.setWorkerName(data[1]);
                worker.setDepartmentNo(data[7]);
                worker.setFlag(1);
                rKey.set(Long.valueOf(worker.getDepartmentNo()));
                context.write(rKey, worker);
            }
        }
    }

    public static class DataReducer extends Reducer<LongWritable, WorkerInfo, LongWritable, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<WorkerInfo> values, Context context) throws IOException, InterruptedException {
            LongWritable resultKey = new LongWritable(0);

            WorkerInfo department = null;
            List<WorkerInfo> workerList = new ArrayList<WorkerInfo>();

            for (WorkerInfo item : values) {
                if (item.getFlag() == 0)
                    department = new WorkerInfo(item);
                else
                    workerList.add(item);
            }

            for (WorkerInfo worker : workerList) {
                worker.setDepartmentNo(department.getDepartmentNo());
                worker.setDepartmentName(department.getDepartmentName());
                result.set(worker.toString());
                context.write(resultKey, result);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: JoinWorkersInformation <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "JoinWorkersInformation");
        job.setJarByClass(JoinWorkerInformation.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(WorkerInfo.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

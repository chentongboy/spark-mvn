package com.kong.hadoop;

import com.kong.hadoop.bean.GroupComparator;
import com.kong.hadoop.bean.MemberKey;
import com.kong.hadoop.bean.WorkerInfo2;
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
 * Join操作调优版
 * Created by kong on 2016/4/27.
 */
public class JoinWorkerInformation2 {
    public static class DataMapper extends Mapper<LongWritable, Text, MemberKey, WorkerInfo2> {
        private MemberKey rKey = new MemberKey();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            String[] data = input.split("\t");

            if (data.length <= 3) {
                WorkerInfo2 department = new WorkerInfo2();
                department.setDepartmentNo(data[0]);
                department.setDepartmentName(data[1]);
                rKey.setKeyId(Integer.parseInt(department.getDepartmentNo()));
                rKey.setFlag(false);
                context.write(rKey, department);
            } else {
                WorkerInfo2 worker = new WorkerInfo2();
                worker.setWorkerNo(data[0]);
                worker.setWorkerName(data[1]);
                worker.setDepartmentNo(data[7]);
                rKey.setKeyId(Integer.parseInt(worker.getDepartmentNo()));
                rKey.setFlag(true);
                context.write(rKey, worker);
            }
        }
    }

    public static class DataReducer extends Reducer<MemberKey, WorkerInfo2, LongWritable, Text> {
        private LongWritable rKey = new LongWritable();
        private Text result = new Text();

        @Override
        protected void reduce(MemberKey key, Iterable<WorkerInfo2> values, Context context) throws IOException, InterruptedException {

            WorkerInfo2 department = null;
            boolean flag = true;

            for (WorkerInfo2 item : values) {
                if (flag)
                    department = new WorkerInfo2(item);
                else{
                    WorkerInfo2 workerInfo = new WorkerInfo2();
                    workerInfo.setDepartmentName(department.getDepartmentName());
                    rKey.set(Long.valueOf(workerInfo.getDepartmentNo()));
                    result.set(workerInfo.toString());
                    context.write(rKey,result);
                }
                flag = false;
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
        job.setJarByClass(JoinWorkerInformation2.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);

        job.setMapOutputKeyClass(MemberKey.class);
        job.setMapOutputValueClass(WorkerInfo2.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(GroupComparator.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

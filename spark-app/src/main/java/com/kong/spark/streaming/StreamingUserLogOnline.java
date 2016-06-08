package com.kong.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 实时日志在线分析系统
 * Created by kong on 2016/5/5.
 */
public class StreamingUserLogOnline {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("userLogsOnline").setMaster("master");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String, String> map = new HashMap<String, String>();
        map.put("metadata.broker.list", "master:9092,worker1:9092,worker2:9092");

        Set<String> topic = new HashSet<String>();
        topic.add("userLogs");
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, map, topic);
        directStream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
                return null;
            }
        });

        //过滤出访问的数据
        JavaPairDStream<String, String> filter = directStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                String[] logs = tuple2._2().split("\t");
                String action = logs[5];
                if ("view".equals(action))
                    return true;
                return false;
            }
        });

        //对pageId进行统计
        JavaPairDStream<Long, Long> count = filter.mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
            public Tuple2<Long, Long> call(Tuple2<String, String> tuple2) throws Exception {
                String[] split = tuple2._2().split("\t");
                Long pageId = Long.valueOf(split[3]);
                return new Tuple2<Long, Long>(pageId, 1L);
            }
        });

        //对pageId进行reduce
        JavaPairDStream<Long, Long> reduce = count.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });

        //一般会把计算数据放入Redis或者DB中
        reduce.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}

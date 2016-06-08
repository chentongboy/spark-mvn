package com.kong.spark.streaming;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Streaming+broadcast+Accumulator实现黑名单过滤与计数
 * Created by kong on 2016/5/12.
 */
public class StreamBroadcastAccu {
    private static volatile Broadcast<List<String>> broadcast = null;
    private static volatile Accumulator<Integer> accumulator = null;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(15));
        //广播黑名单到每个Executor中
        broadcast = jsc.sparkContext().broadcast(Arrays.asList("Hadoop", "Mahout", "Hive"));
        //全局计数器，统计在线过滤了多少个黑名单
        accumulator = jsc.sparkContext().accumulator(0, "OnlineBlackCounter");

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("master", 9999);

        JavaPairDStream<String, Integer> pairDStream = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 2);
            }
        });

        JavaPairDStream<String, Integer> reduce = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        reduce.foreachRDD(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {
            public Void call(JavaPairRDD<String, Integer> pair, Time time) throws Exception {
                pair.filter(new Function<Tuple2<String, Integer>, Boolean>() {
                    public Boolean call(Tuple2<String, Integer> tuple2) throws Exception {
                        if (broadcast.value().contains(tuple2._1())){
                            accumulator.add(tuple2._2());
                            return false;
                        }
                        return true;
                    }
                }).collect();

                System.out.println("BlackList"+broadcast.value().toString() + accumulator.value()+"times");
                return null;
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}

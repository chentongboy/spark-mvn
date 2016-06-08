package com.kong.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kong on 2016/4/24.
 */
public class SparkStreamingKafkaReceiver {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("kafkaReceiver").setMaster("spark://master:7077");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String,Integer> topic = new HashMap<>();
        topic.put("HelloKafka",2);//线程数
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jsc, "master:2181,worker1:2181,worker2:2181", "myConsumerGroup", topic);
        JavaDStream<String> flatMap = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
                return Arrays.asList(tuple._2().split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairDStream = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> reduceByKey = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        reduceByKey.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}


package com.kong.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by kong on 2016/4/16 0016.
 */
public class WordCountOnline {
    public static void main(String[] args) {
        //必须大于等于2，启动起来就是Application，而时间就是另一条线程
        //一条线程不断的循环接受数据，另一条线程处理接受数据
        //每个Exercutor一般肯定不止一个线程，那对于处理Spark Streaming应用程序而言，每个Executor一般分配多少
        //Core比较合适？5个左右Core是最佳的（只有一个Core是运行不出结果的）
        SparkConf conf = new SparkConf().setAppName("WordCountOnline").setMaster("local[2]");

        //创建SparkStreamingContext
        //Driver崩溃后重新启动，由于Spark Streaming具有连续7*24小时不间断运行的特征，所有需要在Driver重新启动后
        //继续上次的状态，此时的状态恢复需要基于曾经的Checkpoint
        //在一个Spark Streaming应用程序中可以创建若干个SparkStreamingContext对象，使用SparkStreaming之前
        //需要把前面正在运行的SparkStreamingContext对象关闭。由此SparkStreaming也只是Spark Core上的一个应用程序而已
        //只不过Spark Streaming框架运行需要Spark工程师的逻辑而已
        JavaStreamingContext streaming = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 创建Spark Streaming输入数据来源input Stream
         * 1.数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
         * 2.这里指定数据来源网络Socket端口，Spark Streaming连接上该端口并监听端口传输的数据
         * 3.如果经常在时间内没有数据的话，不断启动空的job会造成资源的浪费，所以这里需要判断是否空数据
         */
        JavaReceiverInputDStream<String> lines = streaming.socketTextStream("127.0.0.1", 8888);

        /**
         * 基于DStream进行编程
         * 计算前是把Batch的DStream的操作翻译成为RDD操作
         */
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable call(String o) throws Exception {
                return Arrays.asList(o.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2 call(String o) throws Exception {
                return new Tuple2(o, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer o, Integer o2) throws Exception {
                return o + o2;
            }
        });

//        JavaPairDStream<String, Integer> sort = wordCount.sortByKey(false);
//        JavaPairDStream<String, Integer> sortByValue = wordCount.sortByKey(new Comparator<Integer>() {
//            @Override
//            public int compare(Integer o1, Integer o2) {
//                if (o1 > o2)
//                    return 0;
//                else
//                    return 1;
//            }
//        });

//        sort.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> o) throws Exception {
//                System.out.println(o._1() + ":" + o._2());
//            }
//        });

        wordCount.print();
        streaming.start();

        streaming.awaitTermination();
        streaming.close();
    }
}

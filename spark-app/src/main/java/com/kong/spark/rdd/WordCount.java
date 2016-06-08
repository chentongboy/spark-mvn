package com.kong.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Administrator on 2016/1/11.
 */
public class WordCount {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("java WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD lines = sc.textFile("E:/BaiduYunDownload/hadoop/spark-1.6.0-bin-hadoop2.6/README.md");
        JavaRDD words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable call(String o) throws Exception {
                return Arrays.asList(o.split(" "));
            }
        });

        JavaPairRDD pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2 call(String o) throws Exception {
                return new Tuple2(o, 1);
            }
        });

        JavaPairRDD wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer o, Integer o2) throws Exception {
                return o + o2;
            }
        });

        wordCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            public void call(Tuple2<String,Integer> o) throws Exception {
                System.out.println(o._1()+":"+o._2());
            }
        });
    }
}

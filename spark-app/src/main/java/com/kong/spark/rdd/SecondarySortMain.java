package com.kong.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序，具体的实现步骤：
 * 1.按照Ordered和Serializable接口实现自定义排序的Key
 * 2.将要进行二次排序的文件加载进来<Key,Value>类型的RDD
 * 3.使用sortByKey基于自定义的Key进行二次排序
 * 4.去除排序的Key，只保留排序结果
 * Created by kong on 2016/1/24.
 */
public class SecondarySortMain {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("java WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> lines = sc.textFile("D:/a.txt");
        JavaPairRDD pair = lines.mapToPair(new PairFunction<String, SecondarySort, String>() {

            @Override
            public Tuple2<SecondarySort, String> call(String line) throws Exception {
                String[] splited = line.split(" ");
                SecondarySort key = new SecondarySort(Integer.valueOf(splited[0]), Integer.valueOf(splited[1]));
                return new Tuple2<SecondarySort, String>(key, line);
            }
        });
        JavaPairRDD sorted = pair.sortByKey();
        JavaRDD secondarySorted = sorted.map(new Function<Tuple2<SecondarySort, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySort, String> o) throws Exception {
                return o._2();
            }
        });

        secondarySorted.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
        sc.stop();
    }
}

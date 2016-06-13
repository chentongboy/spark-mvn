package com.kong.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 分组TopN
 * Created by kong on 2016/1/25.
 */
public class TopNGroup {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("java TopNGroup").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> lines = sc.textFile("D:/topNGroup.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] splited = s.split(" ");
                return new Tuple2<String, Integer>(splited[0], Integer.valueOf(splited[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> iterable = pairs.groupByKey();//对数据进行分组
        JavaPairRDD<String, Iterable<Integer>> p = iterable.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> groupedData) throws Exception {
                Integer[] top5 = new Integer[5];//保存Top5的数据本身
                String groupedKey = groupedData._1();
                Iterator<Integer> groupedValue = groupedData._2().iterator();
                while (groupedValue.hasNext()) {
                    Integer value = groupedValue.next();//获取当前循环的元素本身的内容
                    for (int i = 0; i < 5; i++) {
                        if (top5[i] == null) {
                            top5[i] = value;
                            break;
                        } else if (value > top5[i]) {
                            for (int j = 4; j > i; j--) {
                                top5[j] = top5[j - 1];
                            }
                            top5[i] = value;
                            break;
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(groupedKey, Arrays.asList(top5));
            }
        });

        p.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> topped) throws Exception {
                System.out.println(topped._1());
                Iterator<Integer> toppedValue = topped._2().iterator();
                while (toppedValue.hasNext()){
                    Integer value = toppedValue.next();
                    System.out.println(value);
                }
                System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
            }
        });
        sc.stop();
    }
}

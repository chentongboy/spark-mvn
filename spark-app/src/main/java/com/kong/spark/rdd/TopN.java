package com.kong.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by kong on 2016/4/23.
 */
public class TopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("top5Java").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, Integer> pairRDD = sc.textFile("c:\\kong\\testData\\top5.txt").mapToPair(new PairFunction<String, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                return new Tuple2<Integer, Integer>(Integer.parseInt(s), Integer.parseInt(s));
            }
        });

        pairRDD.sortByKey(false).map(new Function<Tuple2<Integer,Integer>, Integer>() {
            public Integer call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return integerIntegerTuple2._2;
            }
        }).take(5);
    }
}

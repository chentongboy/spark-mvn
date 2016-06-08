package com.kong.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * RDDTransformation实战案例
 * Created by kong on 2016/4/4 0004.
 */
public class RDDTransformation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("cogroup").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // mapTransformation(sc);
        // filterTransformation(sc);
        //flatMapTransformation(sc);
        //groupByKeyTransformation(sc);
        //reduceByKeyTransformation(sc);
        joinTransformation(sc);
        sc.stop();
    }

    public static void mapTransformation(JavaSparkContext sc) {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i < 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> javaRDD = sc.parallelize(list);
        JavaRDD<Integer> map = javaRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        map.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    public static void filterTransformation(JavaSparkContext sc) {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i < 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> javaRDD = sc.parallelize(list);
        JavaRDD<Integer> filter = javaRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });
        filter.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    public static void flatMapTransformation(JavaSparkContext sc) {
        List<String> data = Arrays.asList("scala spark", "java hadoop", "java tachyon");

        JavaRDD<String> javaRDD = sc.parallelize(data);

        JavaRDD<String> flatMap = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                List<String> iterable = new ArrayList<String>();
                for (String str : split) {
                    iterable.add(str);
                }
                return iterable;
            }
        });

        flatMap.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    public static void groupByKeyTransformation(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2<Integer, String>(100, "spark"), new Tuple2<Integer, String>(100, "tachyon"),
                new Tuple2<Integer, String>(70, "hadoop"), new Tuple2<Integer, String>(80, "kafka"), new Tuple2<Integer, String>(80, "hbase"));
        JavaPairRDD<Integer, String> javaRDD = sc.parallelizePairs(data);
        javaRDD.groupByKey().foreach(new VoidFunction<Tuple2<Integer, Iterable<String>>>() {
            public void call(Tuple2<Integer, Iterable<String>> t) throws Exception {
                System.out.println("key is:" + t._1());
                System.out.println("value is:" + t._2());
            }
        });
    }

    public static void reduceByKeyTransformation(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2<String, Integer>("spark", 100), new Tuple2<String, Integer>("tachyon", 100),
                new Tuple2<String, Integer>("hadoop", 70), new Tuple2<String, Integer>("kafka", 80), new Tuple2<String, Integer>("hbase", 80),
                new Tuple2<String, Integer>("spark",66));
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(data);

        javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println("key is:" + t._1());
                System.out.println("reduce is:" + t._2());
            }
        });
    }

    public static void joinTransformation(JavaSparkContext sc) {
        List<Tuple2<Integer,String>> studentNames = Arrays.asList(new Tuple2<Integer,String>(1, "spark"),new Tuple2<Integer,String>(2, "tachyon"),new Tuple2<Integer,String>(3, "hadoop"));

        List<Tuple2<Integer,Integer>> studentScores = Arrays.asList(new Tuple2<Integer,Integer>(1,100),new Tuple2<Integer,Integer>(2,85),new Tuple2<Integer,Integer>(3,65));

        JavaPairRDD<Integer,String> names = sc.parallelizePairs(studentNames);
        JavaPairRDD<Integer,Integer> scores = sc.parallelizePairs(studentScores);

        names.join(scores).foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("key is:"+t._1());
                System.out.println("name is:"+t._2()._1());
                System.out.println("score is:"+t._2()._2());
                System.out.println("====================================");
            }
        });
    }


    public void coGroup(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> tuple2s = Arrays.asList(new Tuple2<Integer, String>(1, "spark"), new Tuple2<Integer, String>(2, "tachyon"),
                new Tuple2<Integer, String>(3, "hadoop"));

        List<Tuple2<Integer, Integer>> tuples = Arrays.asList(new Tuple2<Integer, Integer>(1, 100), new Tuple2<Integer, Integer>(2, 95),
                new Tuple2<Integer, Integer>(3, 80), new Tuple2<Integer, Integer>(1, 60), new Tuple2<Integer, Integer>(2, 78)
                , new Tuple2<Integer, Integer>(1, 83));

        JavaPairRDD<Integer, String> names = sc.parallelizePairs(tuple2s);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(tuples);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = names.cogroup(scores);

        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println("student Id:" + t._1());
                System.out.println("student Name:" + t._2()._1());
                System.out.println("student Score:" + t._2()._2());
                System.out.println("=========================================");
            }
        });
    }
}

package com.kong.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * Created by lenovo on 2016/6/12.
 */
public class CUAnalysis {
    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("CUAnalysis");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> map = jsc.textFile("/home/kongshaohong/data/20151019-20151025.csv").map(new Function<String, String>() {
            public String call(String s) throws Exception {
                String[] splited = s.split(",");
                String startTime = splited[2];
                if (startTime.length() < 6) {
                    StringBuffer sb = null;
                    for (int i = 0; i < 6 - startTime.length(); i++) {
                        sb.append("0");
                    }
                    startTime = sb.toString() + startTime;
                }
                String longitude = splited[3];
                String latitude = splited[4];
                String areaCode = splited[5];
                String userId = splited[0] + "," + splited[1] + "," + startTime;
                try {
                    if (userId.length() <= 16 || Double.valueOf(longitude) < 113.766667 || Double.valueOf(longitude) > 114.616667 || Double.valueOf(latitude) < 22.450000 || Double.valueOf(latitude) > 22.866667)
                        return null;
                } catch (Exception e) {
                    return null;
                }
                return userId + "," + longitude + "," + latitude + "," + areaCode;
            }
        }).filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return !s.equals("") && s != null;
            }
        });

        //每个用户的记录数
        JavaPairRDD<String, Integer> user = map.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String string) throws Exception {
                String[] split = string.split(",");
                return new Tuple2<String, Integer>(split[0], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //总用户数
        long userCount = map.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<String, Integer>(split[0], 1);
            }
        }).groupByKey().map(new Function<Tuple2<String,Iterable<Integer>>, String>() {
            public String call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                return tuple2._1;
            }
        }).count();

        //总记录数
        long count = map.count();

        //构建用户时间
        JavaPairRDD<String, Long> pairRDD = map.mapToPair(new PairFunction<String, Long, String>() {
            public Tuple2<Long, String> call(String s) throws Exception {
                String[] split = s.split(",");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmmss");
                String time = split[1] + "-" + split[2];
                long timestamp = sdf.parse(time).getTime();
                return new Tuple2<Long, String>(timestamp, split[0]);
            }
        }).sortByKey().mapToPair(new PairFunction<Tuple2<Long, String>, String, Long>() {
            public Tuple2<String, Long> call(Tuple2<Long, String> longStringTuple2) throws Exception {
                return new Tuple2<String, Long>(longStringTuple2._2, longStringTuple2._1);
            }
        });

        //每个用户的平均时间间隔
        JavaPairRDD<String, Long> userAvgTime = pairRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Long>>, String, Long>() {
            public Tuple2<String, Long> call(Tuple2<String, Iterable<Long>> stringIterableTuple2) throws Exception {
                Iterable<Long> longs = stringIterableTuple2._2;
                Iterator<Long> iterator = longs.iterator();
                long count = 0L;
                long sum = 0L;
                long avg = 0L;
                long temp = 0L;
                while (iterator.hasNext()) {
                    if (count == 0) {
                        temp = iterator.next();
                    } else {
                        long next = iterator.next();
                        sum += (next - temp);
                        temp = next;
                    }
                    count++;
                }
                avg = sum / count;
                return new Tuple2<String, Long>(stringIterableTuple2._1, avg);
            }
        });
        
        user.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });
        
        

        jsc.stop();
    }
}

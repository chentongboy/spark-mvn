package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 网站搜索用户热门
 * Created by kong on 2016/5/5.
 */
public class SparkSQLUserLogsHottest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UserLogsHottest").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new HiveContext(sc);
        JavaRDD<String> line = sc.textFile("E:/testData/userLogsForHottest.log");

        String device = "iphone";
        final Broadcast<String> broadcast = sc.broadcast(device);
        JavaRDD<String> filter = line.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains(broadcast.value());
            }
        });

        //回收数据
        //List<String> collect = filter.collect();
        JavaPairRDD<String, Integer> pairRDD = filter.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split("\t");
                int one = 1;
                String dataAndItemAndUserId = split[0] + "#" + split[2] + "#" + String.valueOf(split[1]);
                return new Tuple2<String, Integer>(String.valueOf(dataAndItemAndUserId), Integer.valueOf(one));
            }
        });

        JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        List<Tuple2<String, Integer>> reduceRow = reduceByKey.collect();
        List<String> info = new ArrayList<String>();
        for (Tuple2<String,Integer> row : reduceRow){
            String[] split = row._1().split("#");
            String userId = split[2];
            String item = split[1];
            String date = split[0];
            String json = "{\"date\":\""+date+"\",\"userId\":\""+userId+"\",\"item\":\""+item+"\",\"count\":\""+row._2()+"}";

            info.add(json);
        }

        JavaRDD<String> infoRdd = sc.parallelize(info);

        DataFrame dataFrame = sqlContext.read().json(infoRdd);
        dataFrame.registerTempTable("information");
        String sqlText = "select userId,item,count" + "from("+"select"+"userId,item,count,"+"row_number() over" +
                " (partition by userId order by count DESC) rank" + " from information) sub_info where rank <=3";
        DataFrame result = sqlContext.sql(sqlText);
        result.show();

        result.write().format("json").save("E:/testData/userLogsForHottest");
    }
}

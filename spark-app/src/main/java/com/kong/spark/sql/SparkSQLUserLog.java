package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by kong on 2016/4/7 0007.
 */
public class SparkSQLUserLog {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("userLog").setMaster("spark://master:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        String timestamp = getDays();
        pvStatistic(sqlContext, timestamp);
    }

    private static void pvStatistic(HiveContext hive, String timestamp) {
        hive.sql("use hive");

        String sqlText = "SELECT date,pageId,pv FROM (SELECT date,pageId,count(*) pv FROM " +
                "userLogs WHERE action = 'view' AND date = '" +
                timestamp + "'GROUP BY pageId) subQuery ORDER BY pv DESC";
    }

    private static String getDays() {
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DATE, -2);
        Date twodaysAgo = cal.getTime();
        return twodaysAgo.toString();
    }
}

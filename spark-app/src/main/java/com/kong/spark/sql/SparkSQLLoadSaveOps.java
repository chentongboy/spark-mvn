package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by kong on 2016/3/17 0017.
 */
public class SparkSQLLoadSaveOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrame2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sql = new SQLContext(sc);

        DataFrame df = sql.read().format("json").load("D://person.json");

        df.select("name").write().format("json").save("d://people.json");
    }
}

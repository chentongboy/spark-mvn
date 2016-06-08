package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by kong on 2016/3/14 0014.
 */
public class DataFrameOps {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sparkSQL");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //创建DataFrame，可以简单的认为DataFrame是一张表
        DataFrame df = sqlContext.read().json("");
        //select * from table
        df.show();
        //desc table
        df.printSchema();
        //select name from table
        df.select("name").show();
        //select name,age+1 from table
        df.select(df.col("name"), df.col("age").plus(10)).show();
        //select * from table where age > 10
        df.filter(df.col("age").gt(10)).show();
        //select count(1) from table group by age
        df.groupBy(df.col("age")).count().show();
    }
}

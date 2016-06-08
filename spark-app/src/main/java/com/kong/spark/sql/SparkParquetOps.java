package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created by kong on 2016/3/18 0018.
 */
public class SparkParquetOps {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("parquet").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sql = new SQLContext(sc);

        DataFrame parquet = sql.read().parquet("D://user.parquet");

        parquet.registerTempTable("users");

        DataFrame dataFrame = sql.sql("select * from users");

        JavaRDD<String> name = dataFrame.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "The name is:" + row.getAs("name");
            }
        });

        //List<Row> result = dataFrame.toJavaRDD().collect();
        List<String> result = name.collect();

        for (String row:result){
            System.out.println(row);
        }
    }
}

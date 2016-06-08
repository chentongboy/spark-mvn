package com.kong.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by kong on 2016/3/31 0031.
 */
public class SparkSQLWindowFunction {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("windowFunction").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);

        hiveContext.sql("use hive");
        hiveContext.sql("DROP TABLE IF EXITS scores");
        hiveContext.sql("CREATE TABLE IF NOT EXITS scores(name STRING,score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\\n'");
        hiveContext.sql("LOAD DATA LOCAL INPATH '/root/kong/resources/topNGroup.txt' INTO TABLE scores");

        //使用开窗函数row_number来进行分组排序：
        //PARTITION BY:指定窗口函数分组的key
        //ORDER BY:分组后进行排序
        DataFrame dataFrame = hiveContext.sql("SELECT name,score FROM (SELECT name,score,row_number() OVER (PARTITION BY name ORDER BY score DESC) rank FROM scores) sub_scores WHERE rank <=4");

        dataFrame.show();

        hiveContext.sql("DROP TABLE IF EXITS sortedScore");
        dataFrame.saveAsTable("sortedScore");
    }
}

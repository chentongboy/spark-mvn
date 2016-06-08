package com.kong.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * SparkSQL的窗口实战操作
 * Created by kong on 2016/3/30 0030.
 */
object SparkSQLWindowFunction {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WindowFunction").setMaster("local")
    val sc = new SparkContext(conf)
    val sql = new HiveContext(sc)

    sql.sql("use hive")
    sql.sql("DROP TABLE IF EXITS scores")
    sql.sql("CREATE TABLE IF NOT EXITS scores(name STRING,score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\\n'")
    sql.sql("LOAD DATA LOCAL INPATH '/root/kong/resources/topNGroup.txt' INTO TABLE scores")

    //使用开窗函数row_number来进行分组排序：
    //PARTITION BY:指定窗口函数分组的key
    //ORDER BY:分组后进行排序
    val result = sql.sql("SELECT name,score FROM (SELECT name,score,row_number() OVER (PARTITION BY name ORDER BY score DESC) rank FROM scores) sub_scores WHERE rank <=4")

    result.show()

    sql.sql("DROP TABLE IF EXITS sortedScore")
    result.saveAsTable("sortedScore")

  }
}

package com.kong.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kong on 2016/3/27.
 */
object SparkSQL2Hive {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("2Hive").setMaster("local")
    val sc = new SparkContext(conf)
    //连接Hive
    val sql = new HiveContext(sc)
    sql.sql("user hive")
    sql.sql("DROP TABLE IF EXITS person")
    sql.sql("CREATE TABLE IF NOT EXITS person(name STRING,age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
    sql.sql("LOAD DATA LOCAL INPATH 'E://testData/person.txt' INTO TABLE person")

    sql.sql("DROP TABLE IF EXITS people")
    sql.sql("CREATE TABLE IF NOT EXITS people(name STRING,score INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'")
    sql.sql("LOAD DATA LOCAL INPATH 'E://testData/people.txt' INTO TABLE people")

    val result = sql.sql("SELECT a.name,age,score FROM person a JOIN people b ON a.name=b.name WHERE score>90")

    result.saveAsTable("JOINTable")
    val dataFrame = sql.table("JOINTable")
    dataFrame.show()
  }
}

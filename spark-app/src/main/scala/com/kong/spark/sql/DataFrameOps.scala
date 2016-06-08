package com.kong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kong on 2016/3/14 0014.
 */
object DataFrameOps {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scala DataFrame")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    val df = sql.read.json("hdfs://Master:9000")
    df.show
    df.printSchema
    df.select("name").show
    df.select(df("name"), df("age") + 10).show
    df.filter(df("age") > 10).show
    df.groupBy("age").count.show

    sc.stop()
  }
}

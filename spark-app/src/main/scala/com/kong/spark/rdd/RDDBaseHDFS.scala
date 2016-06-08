package com.kong.spark.rdd

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 使用HDFS方式创建RRD
 * Created by kong on 2016/4/4 0004.
 */
object RDDBaseHDFS {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("baseHDFS") //.setMaster("local")
    val sc = new SparkContext(conf)
    val wordCount = sc.textFile("/library/wordCount/input/license").flatMap(_.split(" ")).map(word => (word, 1))
      .reduceByKey(_ + _).filter(pair => pair._2 > 20).collect().foreach(println)
    sc.stop()
  }
}

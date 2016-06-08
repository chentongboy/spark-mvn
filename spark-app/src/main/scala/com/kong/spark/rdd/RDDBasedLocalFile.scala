package com.kong.spark.rdd

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 使用本地文件创建RDD
 * Created by kong on 2016/4/4 0004.
 */
object RDDBasedLocalFile {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("basedLocalFile").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("E://testData/README.md")

    val lineLength = rdd.map(line => line.length)
    val sum = lineLength.reduce(_ + _)
    println("total file lines:" + sum)
    sc.stop()
  }
}

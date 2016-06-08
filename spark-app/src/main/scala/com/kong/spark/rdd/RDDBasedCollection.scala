package com.kong.spark.rdd

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 使用Collection集合方式创建RDD
 * Created by kong on 2016/4/4 0004.
 */
object RDDBasedCollection {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("basedCollection").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = 1 to 100
    //parallelize方法传入seq和numSlices并行分片数
    val rdd = sc.parallelize(numbers)
    val result = rdd.reduce(_ + _)
    println(result)
    sc.stop()
  }
}

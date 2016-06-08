package com.kong.spark.rdd

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kong on 2016/1/25.
 */
object TopNBasic {
  def main(args: Array[String]) {
    val cof = new SparkConf().setAppName("topN").setMaster("local")
    val sc = new SparkContext(cof)
    val lines = sc.textFile("D:/topN.txt")
    val pairs = lines.map(line => (line.toInt,line))//生成K，V以方便排序
    val sortedPairs = pairs.sortByKey(false)
    val sortedData = sortedPairs.map(pair => pair._2)//过滤出排序的内容本身
    val top5 = sortedData.take(5) //获取排名前五的内容
    top5.foreach(println)
    sc.stop()
  }
}

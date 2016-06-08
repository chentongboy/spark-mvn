package com.kong.spark.rdd

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 二次排序
 * Created by kong on 2016/1/24.
 */
class SecondarySort(val first: Int, val second: Int) extends Ordered[SecondarySort] with Serializable {
  override def compare(that: SecondarySort): Int = {
    if (this.first - that.first != 0)
      this.first - that.first
    else
      this.second - that.second
  }
}

object SecondarySortMain{
  def main(args: Array[String]) {
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Wow,My First Spark App!") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:/a.txt")
    val pairWithSortKey = lines.map(line =>(
      new SecondarySort(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line)
    )

    val sorted = pairWithSortKey.sortByKey(false)

    val sortedResult = sorted.map(s => s._2)

    sortedResult.collect().foreach(println)
    sc.stop()
  }
}

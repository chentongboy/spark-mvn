package com.kong.spark.rdd

import java.text.SimpleDateFormat

import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lenovo on 2016/6/12.
  */
object CUDataAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CUDataAnalysis").setMaster("local")
    val sc = new SparkContext(conf)

    val userData = sc.textFile(args(0)).map(data => {
      val splited: Array[String] = data.split(",")
      var startTime: String = splited(2)
      if (startTime.length < 6) {
        val sb: StringBuffer = null
        sb.append("0" * (6 - startTime.length))
        startTime = sb.toString + startTime
      }
      val longitude: String = splited(3)
      val latitude: String = splited(4)
      val areaCode: String = splited(5)
      val userId: String = splited(0) + "," + splited(1) + "," + startTime
      if (userId.length <= 16 || longitude.toDouble < 113.766667 || longitude.toDouble > 114.616667 || latitude.toDouble < 22.450000 || latitude.toDouble > 22.866667)
        null
      userId + "," + longitude + "," + latitude + "," + areaCode
    }).filter(_ != null)

    //每个用户的记录数
    userData.map(item => (item.split(",")(0), 1)).reduceByKey(_+_)

    //总用户数
    userData.map(item => (item.split(",")(0),1)).groupByKey().map(tuple => (tuple._1,1)).reduceByKey(_+_)

    //总用户数
    userData.count()

    //每个用户的平均时间间隔
    userData.map(item => {
      val array = item.split(",")
      val sdf = new SimpleDateFormat("yyyyMMdd-HHmmss")
      val timestamp = sdf.parse(array(1)+"-"+array(2)).getTime
      (timestamp,array(0))
    }).sortByKey().map(item => (item._2,item._1)).groupByKey().map(item => {
      val it = item._2.iterator
      var count = 0L
      var sum = 0L
      var avg =0L
      var temp =0L
      while (it.hasNext){
        if (count == 0){
          temp = it.next()
        }else{
          val next: Long = it.next
          sum += (next - temp)
          temp = next
        }
        count+=1
      }
      avg = sum/count
      (item._1,avg)
    })

  }
}

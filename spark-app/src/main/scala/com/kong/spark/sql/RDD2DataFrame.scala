package com.kong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
 *
 * Created by kong on 2016/3/15 0015.
 */
object RDD2DataFrame {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    val lines = sc.textFile("D://person.txt")
    import sql.implicits._
    val dataFrame = lines.map(_.split(",")).map(x => People(x(0).toInt,x(1),x(2).toInt)).toDF()
    dataFrame.registerTempTable("peoples")
    val low = sql.sql("select * from peoples where age <10")
    low.map(_.getValuesMap(List("id","name","age"))).collect().foreach(println)

    sc.stop()
  }
}

case class People(var id:Int,var name:String,var age:Int)
package com.kong.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kong on 2016/3/25 0025.
 */
object SparkSQLWithJSON {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    val df = sql.read.json("D://person.json")
    df.registerTempTable("person")
    val excellentStudentDF = sql.sql("select name,score from person where score > 90")
    val excellentStudents = excellentStudentDF.rdd.map(row => row(0)).collect()
    val peopleList = Array("{\"name\":Michael\",\"age\":20}","{\"name\":Andy\",\"age\":17}","{\"name\":Justin\",\"age\":19}")
    val peopleRDD = sc.parallelize(peopleList)
    val peopleDF = sql.read.json(peopleRDD)
    peopleDF.registerTempTable("people")
    val sqlText = "select name,age from people where name in ("
//    for (i <- peopleList.length){
//      sqlText + "'" + peopleList(i) +"'"
//      if (i < peopleList.length-1){
//        sqlText + ","
//      }
//    }
  }
}

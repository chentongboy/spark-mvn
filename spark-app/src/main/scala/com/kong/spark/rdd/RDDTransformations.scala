package com.kong.spark.rdd

import org.apache.spark.{SparkContext, SparkConf}

/**
 * RDDTransformation实战案例
 * Created by kong on 2016/4/4 0004.
 */
object RDDTransformations {
  def main(args: Array[String]) {

    val sc = sparkContext("transformation")

//    mapTransformation(sc)
//
//    filterTransformation(sc)
//
//    flatMapTransformation(sc)
//
//    groupByKeyTransformation(sc)
//
//    reduceByKeyTransformation(sc)

    cogroupTransformation(sc)

    sc.stop()
  }

  def sparkContext(name:String)={
    val conf = new SparkConf().setAppName(name).setMaster("local")
    val sc = new SparkContext(conf)
    sc
  }

  def mapTransformation(sc:SparkContext)={
    val nums = sc.parallelize(1 to 10)
    val mapped = nums.map(item => 2 * item)
    mapped.collect().foreach(println)
  }

  def filterTransformation(sc:SparkContext)={
    val nums = sc.parallelize(1 to 10)
    val filtered = nums.filter(item => item%2 == 0)
    filtered.collect().foreach(println)
  }

  def flatMapTransformation(sc:SparkContext)={
    val data = Array("scala spark","java hadoop","java tachyon")

    val dataRDD = sc.parallelize(data)
    dataRDD.flatMap(item => item.split(" ")).collect().foreach(println)
  }

  def groupByKeyTransformation(sc:SparkContext)={
    val data = Array(Tuple2(100,"spark"),Tuple2(100,"tachyon"),Tuple2(70,"hadoop"),Tuple2(80,"kafka"),Tuple2(80,"hbase"))
    val dataRDD = sc.parallelize(data)
    dataRDD.groupByKey().collect().foreach(println)
  }

  def reduceByKeyTransformation(sc:SparkContext)={
    val data = Array(Tuple2("spark",100),Tuple2("tachyon",100),Tuple2("hadoop",70),Tuple2("kafka",80),Tuple2("hbase",80))
    val dataRDD = sc.parallelize(data)
    dataRDD.reduceByKey(_+_).collect().foreach(println)
  }

  def joinTransformation(sc:SparkContext)={
    val studentNames = Array(Tuple2(1,"spark"),Tuple2(2,"tachyon"),Tuple2(3,"hadoop"))

    val studentScores = Array(Tuple2(1,100),Tuple2(2,85),Tuple2(3,65))

    val names = sc.parallelize(studentNames)
    val scores = sc.parallelize(studentScores)

    names.join(scores).collect().foreach(println)

  }

  def cogroupTransformation(sc:SparkContext)={
    val studentNames = Array(Tuple2(1,"spark"),Tuple2(2,"tachyon"),Tuple2(3,"hadoop"))

    val studentScores = Array(Tuple2(1,100),Tuple2(2,85),Tuple2(3,65),Tuple2(3,66),Tuple2(1,75),Tuple2(3,78))

    val names = sc.parallelize(studentNames)
    val scores = sc.parallelize(studentScores)

    names.cogroup(scores).collect().foreach(println)

  }
}

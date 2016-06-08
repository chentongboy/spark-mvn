package com.kong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.immutable.HashMap

/**
 * Created by kong on 2016/4/24.
 */
object SparkStreamingKafkaReceiver {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("scalaKafkaReceiver").setMaster("spark://master:7077")
    val sc = new StreamingContext(conf, Durations.seconds(5))
    var map = new HashMap[String, Int].empty
    map += ("HelloKafka" -> 2)
    val lines = KafkaUtils.createStream(sc, "master:2181,worker1:2181,worker2:2181", "myConsumerGroup", map)
    lines.flatMap(msg => msg._2.split(" ")).map(word => (word,1)).reduceByKey(_+_).print()
    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}

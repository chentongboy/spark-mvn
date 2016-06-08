package com.kong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.api.java.{JavaStreamingContext, JavaStreamingContextFactory}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * Created by kong on 2016/4/24.
 */
object SparkStreamingOnHDFS {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StreamingOfScalaOnHDFS").setMaster("spark://master:7077")
    //val sc = new StreamingContext(conf,Durations.seconds(5))
    val checkpointDirectory = "hdfs://master:9000/library/SparkStreaming/Checkpoint_Data"
    val sc = StreamingContext.getOrCreate(checkpointDirectory,()=>new StreamingContext(conf,Durations.seconds(5)))

    val lines = sc.textFileStream("hdfs://master:9000/library/SparkStreaming/Data")

    lines.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_).print()

    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}

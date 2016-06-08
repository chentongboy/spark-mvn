package com.kong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用scala开发集群上运行的Spark来实现在线热搜词
  * Created by zhangyuli on 2016/4/30.
  * 背景描述：在社交网络（例如微博）、电子商务（例如京东)、搜索引擎（例如百度）等人们核心关注的内容之一就是我所关注的内容中
  * 大家正在最关注什么或者说当前的热点是什么，这么实际企业级应用中是非常有价值的。例如我们关系过去30分钟大家正在热搜索什么，
  * 并且每5分钟更新一次，这就使得热点内容是动态更新，当然也是更有价值。
  *
  * 实现技术：Spark Streaming提供了滑动窗口的技术来支撑实现上述业务背景，我们，可以使用reductByKeyAndWindow操作来做具体实现
  *
  * 集群运行shell脚本：
  * spark-submit --class com.dt.spark.sparkstreaming.OnlineHottestItems  --master spark://Master:7077 /home/flysky/Documents/scala.jar
  *
  */
object OnlineHottestItems {
  def main(args: Array[String]) {
    val conf = new SparkConf();
    conf.setAppName("OnlineHottestItems");
    conf.setMaster("spark://Master:7077");

    /**
      * 此处设置Batch Interval是在Spark Streaming中生成基本job的时间单位，窗口和滑动时间间隔
      * 一定是改Batch Interval的整数倍
      */
    val ssc = new StreamingContext(conf, Seconds(5));

    ssc.checkpoint("/library/onlinehot/")

    val hottestStream = ssc.socketTextStream("Master", 9999)

    /**
      * 用户搜索的格式简化为name item，在这里我们由于要计算出热点内容，所以只需要提取item即可
      * 提取出的item然后通过map转换为（item，1）
      */
    val searchPair = hottestStream.map(_.split(" ")(1)).map(item => (item, 1))
    //val hottestDStream = searchPair.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(20))

    val hottestDStream = searchPair.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, (v1: Int, v2: Int) => v1 - v2, Seconds(60), Seconds(20))
    //如果操作没有排序，可以通过transform扩展
    hottestDStream.transform(hottestItemRDD => {
      val top3 = hottestItemRDD.map(pair => (pair._2, pair._1)).sortByKey(false).
        map(pair => (pair._2, pair._1)).
        take(3) //选出前3个
      for (item <- top3) {
        println(item)
      }

      hottestItemRDD
    }).print

    /**
      * 计算后的有效数据一般都会写入kafka中，下游的计费系统会从kafka中pull到有效数据进行计费
      */
    ssc.start();
    ssc.awaitTermination();

  }

}

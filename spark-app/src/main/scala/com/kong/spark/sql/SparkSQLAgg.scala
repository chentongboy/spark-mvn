package com.kong.spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

/**
 * 使用SparkSQL中的内置函数对数据进行数据分析，Spark SQL API不同的是，DataFrame中的内置函数操作的结果是返回一个Column对象，
 * 而DataFrame天生就是，这就为了数据的复杂分析建立了坚实的基础并提供了极大的方便性，例如说，我们在操作DataFrame的方法中可以随时
 * 调用内置函数进行业务需要的处理，这之于我们构建附件的业务逻辑而言是可以极大的减少不必要的时间消耗（基于实际模型的映射），让我们
 * 聚焦在数据分析上，这对于提交工程师的生产力而言是非常有价值的
 * 总体而言内置函数包含了五大基本类型：
 * 1.聚合函数，例如countDistinct、sumDistinct
 * 2.集合函数，例如sort_array、explode等
 * 3.日期、时间函数，例如hour、next_day
 * 4.数学函数，例如asin、atan、sqrt等
 * 5.窗口函数，例如rowNumber等
 * Created by kong on 2016/3/28 0028.
 */
object SparkSQLAgg {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("function")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    //使用Spark SQL的内置函数，就一定要导入SQLContext的隐式转换
    import sql.implicits._

    val userData = Array("2016-3-27,003,http://3.apache.org/", "2016-3-27,001,http://1.apache.org/",
      "2016-3-27,002,http://2.apache.org/", "2016-3-27,003,http://1.apache.org/", "2016-3-27,002,http://2.apache.org/",
      "2016-3-28,003,http://3.apache.org/", "2016-3-28,004,http://3.apache.org/", "2016-3-28,004,http://4.apache.org/",
      "2016-3-28,004,http://4.apache.org/", "2016-3-28,001,http://2.apache.org/")

    val dataToRDD = sc.parallelize(userData)
    val dataRow = dataToRDD.map(row => {
      val splited = row.split(","); Row(splited(0), splited(1).toInt, splited(2))
    })
    val structType = StructType(Array(
      StructField("time",StringType,true),
      StructField("id",IntegerType,true),
      StructField("url",StringType,true)
    ))

    val dataDF = sql.createDataFrame(dataRow,structType)

    //注意：内置函数生成的Column对象且自定进行CG
    dataDF.groupBy("time").agg('time,countDistinct('id)).show()

  }
}

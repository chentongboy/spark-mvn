package com.kong.spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * UDF：（User Defined Function）用户自定义的函数，函数的输入是一条具体的数据记录，实现上讲就是普通的
 * Scala函数
 * UDAF：（User Defined Aggregation Function）用户自定义烦人聚合函数，函数本身作用于数据集合
 * 能够在聚合的操作上进行自定义操作
 * 实质上讲，例如说UDF会被Spark SQL中的Catalyst封装成为Expression，最终会通过eval方法来计算输入
 * 的数据ROW（ROW与DataFrame的ROW无关）
 * Created by kong on 2016/3/31 0031.
 */
object SparkSQLUDFUDAF {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("udfAndUdaf").setMaster("local[4]") //设置启动4条线程
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = Array("Spark", "Hadoop", "Spark", "Hadoop", "Hadoop", "Hadoop", "Spark", "Spark", "Hadoop", "Spark", "Hadoop", "Spark", "Spark", "Spark", "Hadoop")

    //基于数据构造DataFrame
    val dataRDD = sc.parallelize(data)
    val dataRDDRow = dataRDD.map(item => Row(item))

    val structType = StructType(Array(StructField("word", StringType, true)))

    val dataDF = sqlContext.createDataFrame(dataRDDRow, structType)

    dataDF.registerTempTable("data") //注册临时表
    //通过SQLContext注册UDF，在Scala2.10.x版本UDF函数最多可以接受22个输入参数
    //缺陷是面向Row一行操作
    sqlContext.udf.register("computeLength", (input: String) => input.length)

    sqlContext.sql("select word,computeLength(word) length from data").show()

    sqlContext.udf.register("wordCount",new MyUDAF)

    sqlContext.sql("select word,wordCount(word) as count,computeLength(word) as length from data group by word").show()

    while (true) () //可以去控制台查看详细运行信息

  }
}

class MyUDAF extends UserDefinedAggregateFunction {
  /**
   * 指定输入数据的类型
   * @return
   */
  override def inputSchema: StructType = StructType(Array(StructField("input", StringType, true)))

  /**
   * 进行Aggregate进行聚合操作的时候所要处理的数据的结果的类型
   * 如：上面的例子，结果是INT
   * @return
   */
  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))

  /**
   * 指定UDAF函数计算后返回的结果类型
   * @return
   */
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  /**
   * 在Aggregate之前每组数据的初始化结果
   * @param buffer
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0) = 0}

  /**
   * 在进行聚合操作的时候每当有新的值进来，对分组后的聚合进行计算
   * 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
   * 如：上面的例子，结果是INT
   * @param buffer
   * @param input
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }


  /**
   *最后在分布式节点进行Local Reduce完成后续进行全局级别的Merge操作
   * @param buffer1
   * @param buffer2
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  /**
   * 返回UDAF最后的计算的结果类型
   * @param buffer
   * @return
   */
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)


}
package com.kong.spark.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;


/***
 * updateStateByKey的主要功能是可以随着时间的流逝在Spark Streaming中为每一个可以维护一份state状态,并且
 * 通过更新函数对该key的状态不断更新,对于每个新的Batch而言,Spark Streaming会在使用updateStateByKey的时候
 * 为已经存在的Key进行State的状态更新(对于每个新出现的Key,会同样的执行state的更新函数操作),但是如果通过更新函数对State更新后返回none的话,此时该Key对应的State会
 * 被删除掉,需要特别说明是State可以是任意类型的数据结构,这就更我们的计算带来无限的想象空间;
 * <p/>
 * 重点来了!如果要不断的更新每个Key的State,就一定涉及到了状态的保存和容错,这个时候就需要开启Checkpoint机制和功能,需要说明的是,Checkpoint可以保存一些可以存储在文件
 * 系统上的内容,例如说程序未处理的数据以及已经拥有的状态;
 * <p/>
 * 补充说明:关于流式处理对历史状态进行保存和更新具有重大是实用意义,例如进行广告点击全面的动态评估
 *
 * @author root
 */
public class UpdateStateByKeyDemo {

    public static void main(String[] args) {

		/*
		 * 第一步：配置SparkConf：
		 * 1，至少2条线程：因为Spark Streaming应用程序在运行的时候，至少有一条
		 * 线程用于不断的循环接收数据，并且至少有一条线程用于处理接受的数据（否则的话无法
		 * 有线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负）；
		 * 2，对于集群而言，每个Executor一般肯定不止一个Thread，那对于处理Spark Streaming的
		 * 应用程序而言，每个Executor一般分配多少Core比较合适？根据我们过去的经验，5个左右的
		 * Core是最佳的（一个段子分配为奇数个Core表现最佳，例如3个、5个、7个Core等）；
		 */
        SparkConf conf = new SparkConf().setMaster("local[2]").
                setAppName("UpdateStateByKeyDemo");
		
		/*SparkConf conf = new SparkConf().setMaster("spark://Master:7077").
				setAppName("WordCountOnline");*/
		
		/*
		 * 第二步：创建SparkStreamingContext：
		 * 1，这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心
		 * SparkStreamingContext的构建可以基于SparkConf参数，也可基于持久化的SparkStreamingContext的内容
		 * 来恢复过来（典型的场景是Driver崩溃后重新启动，由于Spark Streaming具有连续7*24小时不间断运行的特征，
		 * 所有需要在Driver重新启动后继续上衣系的状态，此时的状态恢复需要基于曾经的Checkpoint）；
		 * 2，在一个Spark Streaming应用程序中可以创建若干个SparkStreamingContext对象，使用下一个SparkStreamingContext
		 * 之前需要把前面正在运行的SparkStreamingContext对象关闭掉，由此，我们获得一个重大的启发SparkStreaming框架也只是
		 * Spark Core上的一个应用程序而已，只不过Spark Streaming框架箱运行的话需要Spark工程师写业务逻辑处理代码；
		 */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(15));


        jsc.checkpoint("/usr/Documents/checkpoint");
		
		/*
		 * 第三步：创建Spark Streaming输入数据来源input Stream：
		 * 1，数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等
		 * 2, 在这里我们指定数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口
		 * 		的数据（当然该端口服务首先必须存在）,并且在后续会根据业务需要不断的有数据产生(当然对于Spark Streaming
		 * 		应用程序的运行而言，有无数据其处理流程都是一样的)； 
		 * 3,如果经常在每间隔5秒钟没有数据的话不断的启动空的Job其实是会造成调度资源的浪费，因为并没有数据需要发生计算，所以
		 * 		实例的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job；
		 */

        JavaReceiverInputDStream lines = jsc.socketTextStream("Master", 9999);
		
		/*
		 * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark Streaming具体
		 * 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
		 *对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
	     * 	第4.1步：讲每一行的字符串拆分成单个的单词
	     */
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() { //如果是Scala，由于SAM转换，所以可以写成val words = lines.flatMap { line => line.split(" ")}
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
		
		 /*
	       * 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
	       * 	第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
	       */
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
		
		 /*
	       * 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
	       * 	第4.3步：在这里是通过updateStateByKey来以Batch interval为单位来对历史状态进行更新,这是功能上的一个非常大的改进,
	       * 否则的话要完成同样的目的,就可能需要吧数据保存在Redis或者HDFS或者HBase或者数据库中来不断的完成同样一个Key的State更新
	       * 如果对性能有极为苛刻的要求切数据量特别大的话,可以考虑把数据放在分布式的Redis或者Tachyon内存文件系统中;
	       * 当然,从Spark 1.6.x开始可以尝试实用mapWithState, Spark 2.x后mapWithState应该非常稳定了;
	       */

        JavaPairDStream<String, Integer> wordsCount = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer updatedValue = 0;

                if (state.isPresent()) {
                    updatedValue = state.get();
                }

                for (Integer value : values) {
                    updatedValue += value;
                }

                return Optional.of(updatedValue);
            }


        });
		
		
		/*
		 *	此处的print并不会直接出发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的，对于Spark Streaming
		 *	而言具体是否触发真正的Job运行是基于设置的Duration时间间隔的
		 *	
		 *	诸位一定要注意的是Spark Streaming应用程序要想执行具体的Job，对Dtream就必须有output Stream操作，
		 *	output Stream有很多类型的函数触发，类print、saveAsTextFile、saveAsHadoopFiles等，最为重要的一个
		 *	方法是foraeachRDD,因为Spark Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面，foreachRDD
		 *	主要就是用用来完成这些功能的，而且可以随意的自定义具体数据到底放在哪里！！！
		 *
		 */


        wordsCount.print();
		
		/*
		 * Spark Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
		 * 接受应用程序本身或者Executor中的消息；
		 */
        jsc.start();

        jsc.awaitTermination();
        jsc.close();

    }

}

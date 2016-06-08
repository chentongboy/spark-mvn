package com.kong.spark.rdd

import org.apache.spark.{SparkContext, SparkConf}

import scala.reflect.ClassTag

/**
 * Created by Administrator on 2016/1/9.
 */
object WordCount {
  def main(args: Array[String]) {
    /**
     * 第一步：创建spark的配置对象s、SparkConf，设置Spark程序的运行时的配置信息
     */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Wow,My First Spark App!") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local") //此时，程序在本地运行，不需要安装Spark集群
    //集群模式不用设置
    /**
     * 第二步：创建SparkContext对象
     * SparkContext是spark程序所有功能的唯一入口
     * SparkContext核心作用：初始化spark程序运行时所需的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
     * 同时还会负责Spark程序往Master注册程序等
     * SparkContext是整个spark应用程序中至关重要的一个对象
     */
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行时的具体参数和配置信息
    /**
     * 第三步：根据具体的的数据源通过SparkContext来创建RDD
     * RDD的创建的基本有三种方式：根据外部的数据来源、根据scala集合、由其它的RDD操作
     * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
     */
    val lines = sc.textFile("E:/BaiduYunDownload/hadoop/spark-1.6.0-bin-hadoop2.6/README.md", 1)

    /**
     * 第四步：对初始的RDD进行Transformation级别的处理，例如Map、filter等高阶函数等的编程、来进行具体的数据计算
     * 第4.1步：将每一行的字符串拆分成的单个的单词
     */
    val words = lines.flatMap(x => x.split(" ")) //切分每一行的单词，并通过flat合并成为一个大的单词集合
    /**
     * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word,1)
     */
    val pairs = words.map(word => (word, 1))

    /**
     * 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
     */
    val wordCounts = pairs.reduceByKey(_ + _) //对相同的Key，进行Value的累计

    //作业、排序
    val sortWordByKey = wordCounts.sortByKey(false, 1)
    sortWordByKey.foreach(wordNumPair => println(wordNumPair._1 + ":" + wordNumPair._2))

    val sortWordByValue = wordCounts.sortBy(word => word._2, true, 1)
    sortWordByValue.foreach(wordNumPair => println(wordNumPair._1 + ":" + wordNumPair._2))
    sc.stop()
  }
}
/**
//对array进行模式匹配
def data(array: Array[String]): Unit = {
  array match {
    case Array("scala") => println("scala") //只有scala一个元素的array
    case Array(val1, val2, val3) => println(val1 + ":" + val2 + ":" + val3) //有三个元素的array
    case Array("spark", _*) => println("spark ...") //含有spark的array
    case _ => println("Unknown")
  }
}

//对class进行模式匹配
class Person
//case class 相当于javaBean，默认是只读成员，所以特别适合于并发编程时的消息通讯
//没有指定var与val，scala会自动用val修饰，一般不指定var，因为不符合消息通讯体设计的目的
//实际工作的时候，会生成class的伴生对象object，默认生成apply方法，然后构建成Person的实例
case class Worker(name:String,salary:Double) extends Person
case class Student(name:String,score:Double) extends Person
def Info(person: Person): Unit ={
  person match{
    case Student(name,score) => println("student:"+name+","+score)
    case Worker(name,salary) => println("worker:"+name+","+salary)
    case _ => println("others")
  }
}

//泛型
//确保类或者函数定义类型参数的时候，就说明操作时一定是这种类型
class People[T](val content:T){
  def getContent(id:T) = id +":"+ content
}

val p = new People[String]("Spark")
p.getContent("Scala")

//上边界(<:)
//大数据工程师（泛型，有各种数据工程师），限定大数据工程师的类型就需要边界，例如这个工程师至少要掌握spark，但是你可以
//掌握其它大数据的技能hadoop等等。这些其它技能就是子类的事情，但是确认你有父类的的方法即spark技能。
//下边界(>:)
//指定了我们泛型类型必须是某个类的父类或者类本身类型

//View Bounds 视图界定
//某个类型既没有上边界也没有下边界，
class Compare[T :Ordering](val n1:T,val n2:T){
  def bigger(implicit ordered: Ordering[T]) = if (ordered.compare(n1,n2) > 0 ) n1 else n2
}

val big = new Compare[Int](8,3).bigger
val bigger = new Compare[String]("Spark","Hadoop").bigger

//Manifest Context Bounds
//对泛型数组有用,定义的时候[T:Manifest],这可以实例Array[T]
//现在使用的是ClassTag
def mkArray[T :ClassTag](elems:T*)=Array[T](elems: _*)
//mkArray: [T](elems:T*)(implicit evidence$1: scala.reflect.ClassTag[T])Array[T]
mkArray(42,13) //结果：Array[Int] = Array(42,13)
mkArray("Scala","Spark","Hadoop") //结果：Array[String] = Array(Scala,Spark,Hadoop)

//协变
class Person[+T]
//有加号就是协变，父类和子类之间的继承关系在泛型中是一个加号，那么我们利用这个协变逆变就可以变成
//类型本身是子类父类关系的话，那实际上运行的时候类型本身作为参数的类也会构成父类和子类，如果继承方式是一致的话就是协变，不一致就是逆变
/**
 * C[+T]:如果A是B的子类，那么C[A]是C[B]的子类。 协变范围大
 * C[-T]：如果A是B的子类，那么C[B]是C[A]的子类。 逆变范围小
 * C[T]：无论A和B是什么关系，C[A]和C[B]没有从属关系。
 */
//Dependency[_]相当于Dependency[T]
  */
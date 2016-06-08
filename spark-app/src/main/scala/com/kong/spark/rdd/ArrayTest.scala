package com.kong.spark.rdd

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2016/1/1.
 */
object ArrayTest {
  def main(args: Array[String]) {
    val array = ArrayBuffer(1,2,3,-4,-5,-52,1,5,8,-4,-48,26,11)
    val array2 = ArrayBuffer[Int]()
    var flag = 0
    var flag2 =0
    var count =0
    for (i <- 0 until array.length){
      if (array(i) < 0) {
        flag +=1
        if (flag > 1) {
          flag=1
          array2 +=i
        }
      }
    }
    println(array)
    //print(flag)
    println(array2)
    val array3=array2.sortWith(_-_>0)
    for (elem <- array3) {
      array.remove(elem)
    }
    println(array)
  }
}

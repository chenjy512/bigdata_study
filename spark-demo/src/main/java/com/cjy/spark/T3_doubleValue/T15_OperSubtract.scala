package com.cjy.spark.T3_doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * subtract：取出两个rdd中相同的元素，将调用的rdd中的其他不同元素保存下来
  */
object T15_OperSubtract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 4)
    //
    val mkRDD2: RDD[Int] = sc.makeRDD(3 to 6)
    //去重与源rdd相同的元素
    val subtract: RDD[Int] = mkRDD.subtract(mkRDD2)
    val sub2: RDD[Int] = mkRDD2.subtract(mkRDD)
    subtract.foreach(println) //1 2
    println("***********************")
    sub2.foreach(println) //5 6
  }
}

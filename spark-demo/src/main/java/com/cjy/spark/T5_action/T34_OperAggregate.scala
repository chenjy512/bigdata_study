package com.cjy.spark.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregate:效果与kv的一样，但是不一样之处在于，kv分区间调用时不用初始化值，二单value类型区间合并一样调用初始化值
  */
object T34_OperAggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 10,2)
    //收集rdd前4个元素
    val sum1: Int = mkRDD.aggregate(0)(_ + _, _ + _)
    println(sum1) //55
    val sum2: Int = mkRDD.aggregate(10)(_ + _, _ + _)
    println(sum2) //85
    /**
      * 为什么是85？
      * 原因：首先1 to 10 和是55
      *       其次有两个分区，所以加上初始值10*2
      *       再者分区间合并，再加上初始值10，总和是85
      */

  }
}

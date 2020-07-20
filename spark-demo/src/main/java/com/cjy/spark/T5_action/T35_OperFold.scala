package com.cjy.spark.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * fold：折叠操作，当分区内与分区间操作一样时
  */
object T35_OperFold {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 10,2)

    val sum1: Int = mkRDD.fold(0)(_+_)
    println(sum1) //55
    val sum2: Int = mkRDD.fold(10)(_+_)
    println(sum2) //85
    //原因同 aggregate一样，先分区再分区间
  }
}

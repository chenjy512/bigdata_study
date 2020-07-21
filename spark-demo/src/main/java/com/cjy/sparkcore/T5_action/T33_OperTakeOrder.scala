package com.cjy.sparkcore.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * take(num: Int):返回排序后的RDD前num个元素
  */
object T33_OperTakeOrder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(List(5,3,9,2,5,1))
    //收集rdd前4个元素
    val takeRDD: Array[Int] = mkRDD.takeOrdered(4)
    takeRDD.foreach(println)
  }
}

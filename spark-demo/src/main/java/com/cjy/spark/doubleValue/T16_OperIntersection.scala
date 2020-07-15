package com.cjy.spark.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * intersection：计算两个rdd的交集
  */
object T16_OperIntersection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 4)
    //
    val mkRDD2: RDD[Int] = sc.makeRDD(3 to 6)
    //求两个rdd的交集
    val unionRDD: RDD[Int] = mkRDD.intersection(mkRDD2)
    unionRDD.foreach(println)
  }
}

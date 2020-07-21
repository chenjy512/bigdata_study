package com.cjy.sparkcore.T3_doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * cartesian：两个rdd的笛卡尔积
  */
object T17_OperCartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 3)
    //
    val mkRDD2: RDD[Int] = sc.makeRDD(3 to 5)
    //求两个rdd的笛卡尔积
     val cartesian: RDD[(Int, Int)] = mkRDD.cartesian(mkRDD2)
    cartesian.foreach(println)
  }
}

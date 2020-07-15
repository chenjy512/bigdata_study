package com.cjy.spark.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * zip：将两个rdd数据合并，组成tuple2 数据类型，效果同scala集合zip一样
  */
object T18_OperZip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 3)
    val mkRDD2: RDD[String] = sc.makeRDD(Array("a", "b", "c"))
    //
    val zipRDD: RDD[(Int, String)] = mkRDD.zip(mkRDD2)
    zipRDD.foreach(println)
  }
}

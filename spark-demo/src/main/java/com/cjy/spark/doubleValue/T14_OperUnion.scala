package com.cjy.spark.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * union：对源RDD和参数RDD求并集后返回一个新的RDD
  */
object T14_OperUnion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 3)
    //
    val mkRDD2: RDD[Int] = sc.makeRDD(3 to 6)
    //求两个rdd的并集，也就是两个rdd数据合并
    val unionRDD: RDD[Int] = mkRDD.union(mkRDD2)
    unionRDD.foreach(println)
  }
}

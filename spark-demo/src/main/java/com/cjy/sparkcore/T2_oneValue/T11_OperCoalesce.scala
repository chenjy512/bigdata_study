package com.cjy.sparkcore.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * coalesce：减少分区
  */
object T11_OperCoalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(List(1,3,4,2,3,4,2,3,2,2,1,5,6,4,2),4)
    // 重新设置分区
    val coalesceRDD = mkRDD.coalesce(2)
    println(coalesceRDD.partitions.size)
  }
}

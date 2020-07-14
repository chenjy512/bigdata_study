package com.cjy.spark.oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * glom：将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
  */
object T6_OperGlom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(1 to 12,4)
    val glomRDD = mkRDD.glom()
    //每个分区数据是一个数组
    glomRDD.collect().foreach(x => {
      val str = x.mkString(",")
      println(str)
    })
  }
}

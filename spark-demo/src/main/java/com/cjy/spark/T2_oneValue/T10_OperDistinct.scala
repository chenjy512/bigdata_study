package com.cjy.spark.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * distinct：数据去重，可以选择并行度
  */
object T10_OperDistinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(List(1,3,4,2,3,4,2,3,2,2,1,5,6,4,2))
    //数据去重复
    val distinct = mkRDD.distinct(2)
    distinct.collect().foreach(println)
  }
}

package com.cjy.spark.oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * filter：过滤。返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成。
  */
object T8_OperFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(1 to 12)
    //将 取模2 等于0的元素留下
    val filter = mkRDD.filter(x => x % 2 ==0)
    filter.collect().foreach(println)
  }
}

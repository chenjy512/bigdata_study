package com.cjy.spark.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap：类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素
  */
object T5_OperFlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(Array(List(1 , 3),List(2 , 4)))
    val flatRDD = mkRDD.flatMap(x => x)
    flatRDD.collect().foreach(println)
  }
}

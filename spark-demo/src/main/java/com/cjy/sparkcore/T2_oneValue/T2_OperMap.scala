package com.cjy.sparkcore.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * map算子：与scala集合中的map效果一样；返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
  *
  * 需求：通过map将元素 * 2
  */
object T2_OperMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(1 to 5)
    val mapRDD = mkRDD.map(_*2)
    mapRDD.collect().foreach(println)
  }
}

package com.cjy.spark.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * reduce：效果同sacla集合一样，但是解释不一样这里是先聚合分区内数据，再聚合分区间数据；
  *   而scala数据是在一个集合容器中
  */
object T29_OperReduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 10)
    val count: Int = mkRDD.reduce(_+_)
    println(count)
  }
}

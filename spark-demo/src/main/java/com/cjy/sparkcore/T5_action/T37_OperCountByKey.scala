package com.cjy.sparkcore.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * countByKey：返回一个(K,Int)的map，表示每一个key对应的元素个数。
  */
object T37_OperCountByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[(Int, Int)] = sc.makeRDD(Array((1,2),(1,4),(3,4)))
    val crd: collection.Map[Int, Long] = mkRDD.countByKey()
    crd.foreach(println(_))
  }
}

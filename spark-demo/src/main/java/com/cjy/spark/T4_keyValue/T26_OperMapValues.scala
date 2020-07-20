package com.cjy.spark.T4_keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapValues：只针对value做map操作
  */
object T26_OperMapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[(Int,Int)] = sc.makeRDD(Array((4,2),(6,3),(1,3),(2,5)),2)
    val s1: RDD[(Int, Int)] = mkRDD.mapValues(_*10)
    s1.collect().foreach(println)
  }
}

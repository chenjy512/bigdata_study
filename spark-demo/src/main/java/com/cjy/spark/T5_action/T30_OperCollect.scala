package com.cjy.spark.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * collect：将RDD数组收集到scala数组容器中
  */
object T30_OperCollect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //将rdd数据收集到一个数组中，注意此数组是scala中的不在是包装的RDD了
    val coll: Array[Int] = mkRDD.collect()
    //这里调用就是scala数组函数
    val count: Int = coll.reduce(_+_)
    println(count)
  }
}

package com.cjy.spark.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * take(num: Int):返回RDD前num个元素，如果num 是rdd中元素和，那么效果就与collect一样了
  */
object T32_OperTake {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //收集rdd前4个元素
    val takeRDD: Array[Int] = mkRDD.take(4)
    takeRDD.foreach(println(_))
  }
}

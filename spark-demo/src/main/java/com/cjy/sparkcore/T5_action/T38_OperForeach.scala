package com.cjy.sparkcore.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * foreach：在数据集的每一个元素上，运行函数func进行更新。
  */
object T38_OperForeach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 10,2)
    //注意这里是rdd的foreach，是运行在 Executor 中的，而scala的foreach是运行在driver中的
    mkRDD.foreach(println(_))
  }
}

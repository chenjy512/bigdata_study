package com.cjy.spark.T4_keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * reduceByKey：将key相同聚合到一起
  */
object T21_OperReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[String] = sc.makeRDD(Array("one", "two", "two", "three", "three", "three"))
    //数据转为 k-v 类型
    val mapRDD: RDD[(String, Int)] = mkRDD.map((_,1))
    //将key相同元素聚集到同一sequence 中
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey((x, y) => x+y)
    reduceRDD.collect().foreach(println(_))
  }
}

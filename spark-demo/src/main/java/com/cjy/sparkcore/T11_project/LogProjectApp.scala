package com.cjy.sparkcore.T11_project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object LogProjectApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("log-deal")
    val sc:SparkContext = new SparkContext(conf)
    //1. 读取文件
    val textRDD: RDD[String] = sc.textFile("/Users/chenjunying/Downloads/user_data.txt")
    //    println(textRDD.collect().length)
    //2. 数据类型转换
    val mapRDD: RDD[UserVisitAction] = textRDD.map(x => {
      val splits: Array[String] = x.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })
    //每个品类订单前十名
    val topData: List[CategoryCountInfo] = LogNeedsApp.dealNeedCategorTop10(sc,mapRDD)
    for (elem <- topData) {
      println(elem)
    }
  }


}

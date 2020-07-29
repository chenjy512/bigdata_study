package com.cjy.sparkcore.T11_project
import com.cjy.sparkcore.T11_project.need1.CategoryTop10App
import com.cjy.sparkcore.T11_project.need2.{CategorySessionApp, CategorySessionApp2, CategorySessionApp3}
import com.cjy.sparkcore.T11_project.need3.PageConversionApp
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object LogProjectApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("log-deal")
    val sc:SparkContext = new SparkContext(conf)
    //1. 读取文件
    //    val textRDD: RDD[String] = sc.textFile("D:\\baidunyunDownLoad\\data\\")
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

    val topData: List[CategoryCountInfo] = CategoryTop10App.dealNeedCategorTop10(sc,mapRDD)
    for (elem <- topData) {
      println(elem)
    }

    println("-------------需求实现二----------")

    //     CategorySessionApp.queryCategorySessionTop10(sc,mapRDD,topData)
    //     CategorySessionApp2.queryCategorySessionTop10(sc,mapRDD,topData)
    //     CategorySessionApp3.queryCategorySessionTop10(sc,mapRDD,topData)
    PageConversionApp.calcPageConversion(sc,mapRDD,"1,2,3,4,5,6")
  }


}

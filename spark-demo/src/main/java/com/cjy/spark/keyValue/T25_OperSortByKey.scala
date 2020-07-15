package com.cjy.spark.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortByKey：按照key排序
  */
object T25_OperSortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[(Int,Int)] = sc.makeRDD(Array((4,2),(6,3),(1,3),(2,5)),2)
    //默认升序
    val s1: RDD[(Int, Int)] = mkRDD.sortByKey()
    s1.collect().foreach(println)
    //false：降序
    val s2: RDD[(Int, Int)] = mkRDD.sortByKey(false)
    s2.collect().foreach(println)
    //true：升序
    val s3: RDD[(Int, Int)] = mkRDD.sortByKey(true)
    s3.collect().foreach(println)
  }
}

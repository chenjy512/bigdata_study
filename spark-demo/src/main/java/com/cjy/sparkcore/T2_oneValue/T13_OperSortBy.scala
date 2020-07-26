package com.cjy.sparkcore.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortBy：使用func先对数据进行处理，按照处理后的数据比较结果排序，默认为正序。
  *
  *
  */
object T13_OperSortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(List(1,3,4,2,3,4,2,3,2,2,1,5,6,4,2),4)
    //按照数据本身大小排序
    val sortRDD = mkRDD.sortBy(x => x)
    sortRDD.collect().foreach(println)
    println("---------------以下是scala排序，后面是排序类型-------------------")
    val ints: Array[Int] = mkRDD.collect().sortBy(x => x)(Ordering.Int.reverse)
    for (elem <- ints) {
      println(elem)
    }
  }
}

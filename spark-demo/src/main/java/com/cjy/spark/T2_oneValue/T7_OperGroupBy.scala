package com.cjy.spark.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupBy：分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器
  */
object T7_OperGroupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(1 to 12)
    //将 每个元素取模2 后相等的只放在一组
    val groupRDD = mkRDD.groupBy(x => x % 2)
    groupRDD.collect().foreach(println)
  }
}

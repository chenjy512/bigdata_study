package com.cjy.spark.T4_keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * foldByKey：针对区内，区间相同操作
  */
object T22_OperAggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aggregate").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val mkRDD: RDD[(String,Int)] = sc.makeRDD(Array(("two",12),("three",3), ("two",2), ("two",3), ("three",4), ("three",3)),2)
    mkRDD.glom().collect().foreach(x => println( x.mkString(",")))
    //分区内，分区间操作相同时使用
    val a2: RDD[(String, Int)] = mkRDD.foldByKey(0)(_+_)

    a2.collect().foreach(println)

  }
}

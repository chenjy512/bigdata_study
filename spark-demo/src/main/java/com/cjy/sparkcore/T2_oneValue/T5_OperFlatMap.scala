package com.cjy.sparkcore.T2_oneValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap：类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素
  */
object T5_OperFlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(Array(List(1, 3), List(2, 4)))
    val flatRDD = mkRDD.flatMap(x => x)
    flatRDD.collect().foreach(println)

    val m2: RDD[(Int, List[(Int, Int)])] = sc.makeRDD(Array((1, List((1, 3),(7, 8))), (2, List((4, 5)))))
    val f2: RDD[(Int, Int)] = m2.flatMap(x =>x._2)
//    f2.collect().foreach(println)

    val f3 = m2.flatMap {
      case (id, its) => {
        its.map(x => x._2)
      }
    }.collect().foreach(println)

  }
}

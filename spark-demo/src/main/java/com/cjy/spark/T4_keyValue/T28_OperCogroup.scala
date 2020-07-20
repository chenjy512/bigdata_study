package com.cjy.spark.T4_keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * cogroup：同join类似，不同的是匹配不上的一样存在，并设置默认值
  */
object T28_OperCogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd1 = sc.parallelize(Array((5, 4), (2, 5), (3, 6)))

    val c1: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd1)
    c1.collect().foreach(println)

    /**
      * (1,(CompactBuffer(a),CompactBuffer()))
      * (2,(CompactBuffer(b),CompactBuffer(5)))
      * (3,(CompactBuffer(c),CompactBuffer(6)))
      * (5,(CompactBuffer(),CompactBuffer(4)))
      */
  }
}

package com.cjy.spark.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * join：连表，key相同的聚合，只在同一个rdd中存在key则过滤
  */
object T27_OperJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))
    val rdd2 = sc.parallelize(Array((6, 4), (2, 5), (3, 6)))
    //连表操作
    val joinRDD: RDD[(Int, (String, Int))] = rdd.join(rdd1)
    joinRDD.collect().foreach(println)

    /**
      * (1,(a,4))
      * (2,(b,5))
      * (3,(c,6))
      */

    val joinRDD2: RDD[(Int, (String, Int))] = rdd.join(rdd2)
    joinRDD2.collect().foreach(println)

    /**
      * (2,(b,5))
      * (3,(c,6))
      */
  }
}

package com.cjy.sparkcore.T7_other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *cache：将当前rdd结果缓存，提高效率
  */
object CacheTest {
  def main(args: Array[String]): Unit = {
    //1.配置
    val conf = new SparkConf().setAppName("cache").setMaster("local[1]")
    //2.创建上下文对象
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array("ccl"))
    val nocache = rdd.map(_.toString+System.currentTimeMillis)

    nocache.collect.foreach(println(_))
    Thread.sleep(1000)
    nocache.collect.foreach(println(_))
    Thread.sleep(1000)
    nocache.collect.foreach(println(_))
    //以上三次结果不同
    //以下缓存结果查看
    val cache: RDD[String] = nocache.cache()
    cache.collect.foreach(println(_))
    Thread.sleep(1000)
    cache.collect.foreach(println(_))
    Thread.sleep(1000)
    cache.collect.foreach(println(_))
  }
}

package com.cjy.spark.T7_other

import org.apache.spark.{SparkConf, SparkContext}

/**
  * ChechkPoint：检查点也就是将当前rdd数据保存在设置的路径下，当下次使用时避免从头开始转换计算，从此路径读取数据即可
  */
object ChechkPointTest {
  def main(args: Array[String]): Unit = {
    //1.配置
    val conf = new SparkConf().setAppName("cache").setMaster("local[1]")
    //2.创建上下文对象
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("spark-demo/checkpoint")
    val rdd = sc.parallelize(Array("ccl"))
    val ch = rdd.map(_+System.currentTimeMillis)
    //设置检查点
    ch.checkpoint

    ch.collect.foreach(println(_))
    Thread.sleep(1000)
    ch.collect.foreach(println(_))
    Thread.sleep(1000)
    ch.collect.foreach(println(_))
  }
}

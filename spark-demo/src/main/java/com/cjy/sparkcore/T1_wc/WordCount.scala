package com.cjy.sparkcore.T1_wc

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main( args: Array[String]): Unit = {

    val in = "spark-demo/in"
    val out = "spark-demo/out"

    //1.配置
//    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val conf = new SparkConf().setAppName("wc")
    //2.创建上下文对象
    val sc = new SparkContext(conf)
    //3 读取目录
    val file = sc.textFile(args(0))
    //4 每行数据按照 空格 分割
    val words = file.flatMap(_.split(" "))
    //5 数据设置成 tuple类型，根据key聚合
    val count = words.map((_,1)).reduceByKey(_+_)
    //6 保存到指定路径
    count.saveAsTextFile(args(1))

    sc.stop()
  }
}

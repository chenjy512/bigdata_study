package com.cjy.spark.T7_other

import org.apache.spark.{SparkConf, SparkContext}

/**
  * toDebugString：依赖关系查看
  *   窄依赖：一对一
  *   宽依赖：一对多，产生shuffer，也就是stage划分
  */
object DependencyTest {
  def main(args: Array[String]): Unit = {
    val in = "spark-demo/in"
    val out = "spark-demo/out"

    //1.配置
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    //2.创建上下文对象
    val sc = new SparkContext(conf)
    //3 读取目录
    val file = sc.textFile(in)
    //4 每行数据按照 空格 分割
    val words = file.flatMap(_.split(" "))
    //5 数据设置成 tuple类型，根据key聚合
    val count = words.map((_,1)).reduceByKey(_+_)

    val debug: String = count.toDebugString
    println(debug)

    sc.stop()
  }
}

package com.cjy.sparkcore.T9_saveData

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * saveAsObjectFile：对象文件是将对象序列化后保存的文件，采用Java的序列化机制。可以通过objectFile[k,v](path) 函数接收一个路径，
  * 读取对象文件，返回对应的 RDD，也可以通过调用saveAsObjectFile() 实现对对象文件的输出。因为是序列化所以要指定类型。
  *
  */
object T4_OperObjectFile {
  def main(args: Array[String]): Unit = {
    //1.配置
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    //2.创建上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(1,2,3,4))
    //数据存盘
    rdd.saveAsObjectFile("spark-demo/object")

    //数据读取
    val objRDD: RDD[Int] = sc.objectFile[Int]("spark-demo/object")
    objRDD.collect().foreach(println)
  }

}

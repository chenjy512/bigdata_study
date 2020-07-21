package com.cjy.sparkcore.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD创建：两种方式内部与外部
  *   内部：parallelize和makeRDD
  *   外部：读取文件
  */
object T1_RDDCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //1.内部创建：makeRDD 接收一个序列，其实make底层调用的也是parallelize函数
    val makeRDD = sc.makeRDD(1 to 10)
    makeRDD.collect().foreach(println)
    //内部创建: parallelize()从集合创建
    val pRDD = sc.parallelize(1 to 5)
    pRDD.collect().foreach(println)

    //2. 外部创建
    val textRDD = sc.textFile("spark-demo/in")
    textRDD.collect().foreach(println)
  }
}

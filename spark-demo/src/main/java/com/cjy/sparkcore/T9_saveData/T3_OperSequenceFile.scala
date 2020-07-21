package com.cjy.sparkcore.T9_saveData

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * saveAsSequenceFile：SequenceFile文件是Hadoop用来存储二进制形式的key-value对而设计的一种平面文件(Flat File)。
  *                       Spark 有专门用来读取 SequenceFile 的接口。在 SparkContext 中，
  *                       可以调用 sequenceFile[ keyClass, valueClass](path)。
  */
object T3_OperSequenceFile {
  def main(args: Array[String]): Unit = {
    //1.配置
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    //2.创建上下文对象
    val sc = new SparkContext(conf)

    val mkRDD= sc.makeRDD(Array((1,2),(3,4),(5,6)))
    //数据存盘
//    mkRDD.saveAsSequenceFile("spark-demo/sequence")

    //文件数据读取
      val seqRDD: RDD[(Int, Int)] = sc.sequenceFile[Int,Int]("spark-demo/sequence")
      seqRDD.collect().foreach(println)
  }

}

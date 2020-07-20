package com.cjy.spark.T5_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * saveAsTextFile(path)：将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，
                          Spark将会调用toString方法，将它装换为文件中的文本
  * saveAsSequenceFile(path)：将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，
                                可以使HDFS或者其他Hadoop支持的文件系统。
  * saveAsObjectFile(path)：用于将RDD中的元素序列化成对象，存储到文件中。
  */
object T36_OperSaveAsText {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-action").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[Int] = sc.makeRDD(1 to 10,1)

    mkRDD.saveAsTextFile("spark-demo/out1")
    mkRDD.saveAsObjectFile("spark-demo/out2")

    val mk2: RDD[(Int, Int)] = sc.makeRDD(Array((1,2),(1,4),(3,4)))
    mk2.saveAsSequenceFile("spark-demo/out3")
  }
}

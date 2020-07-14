package com.cjy.spark.oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapPartitionsWithIndex：类似于mapPartitions，但func带有一个整数参数表示分片的索引值，
  *         因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U]
  */
object T4_OperMapPartitionIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(1 to 5)
    val mapPartionIndexRDD = mkRDD.mapPartitionsWithIndex((index, datas) => datas.map((index, _)))
    mapPartionIndexRDD.collect().foreach(println)
  }
}

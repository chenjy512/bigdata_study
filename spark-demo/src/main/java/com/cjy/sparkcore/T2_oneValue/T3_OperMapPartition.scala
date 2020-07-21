package com.cjy.sparkcore.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * mapPartitions：每个分区元素组成一个序列为一个元素，map是所有元素在一个序列，所以map是N个元素调用N次，mapPartitions M个分区调用M次
  *   mapPartitions调用次数少则效率高，但是由于每个分区元素在一起所以有内存不够的风险 OOM
  *
  * 需求：通过map将元素 * 2
  */
object T3_OperMapPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(1 to 5)
    val mapPartitionRDD = mkRDD.mapPartitions(datas => datas.map(_*2))
    //datas 是一个分区的元素
    mapPartitionRDD.collect().foreach(println)
  }
}

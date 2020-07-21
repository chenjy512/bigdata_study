package com.cjy.sparkcore.T8_partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext}

/**
  * partitionBy：自定义分区器，指定分区规则
  */
object T19_OperPartionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //16 个元素，4个人去
    val mkRDD: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(4,"d")),4)
    //重新分区
    val pRDD: RDD[(Int, String)] = mkRDD.partitionBy(new MyPartiotion(2))
    println( pRDD.partitions.size)
    pRDD.saveAsTextFile("spark-demo/out")
  }
}

/**
  * 自定义分区类
  * @param num  指定分区个数
  */
class MyPartiotion(num:Int) extends Partitioner{
  //获取分区数
  override def numPartitions: Int = {
        num
  }
  //指定数据分区规则
  override def getPartition(key: Any): Int = {
        if(key.toString.toInt > 3){
          1
        }else{
          0
        }
  }
}
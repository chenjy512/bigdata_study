package com.cjy.sparkstreaming.T2_DStreamCreate

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 测试过程中，可以通过使用ssc.queueStream(queueOfRDDs)来创建DStream，每一个推送到这个队列中的RDD，都会作为一个DStream处理。
  */
object T1_RDDQueueDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDQueueDemo").setMaster("local[*]")
    val scc = new StreamingContext(conf, Seconds(5))
    val sc = scc.sparkContext
    //rdd容器
    val queueRDD: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()

    val rddDs: InputDStream[Int] = scc.queueStream(queueRDD,true)
    //数据计算
    rddDs.reduce(_+_).print()

    scc.start()

    for (elem <- 1 to 10){
      queueRDD += sc.makeRDD(1 to 100)
      Thread.sleep(100)
    }
    //每次处理一个rdd

    scc.awaitTermination()
  }
}

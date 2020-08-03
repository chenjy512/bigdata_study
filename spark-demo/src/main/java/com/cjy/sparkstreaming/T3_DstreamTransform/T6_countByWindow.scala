package com.cjy.sparkstreaming.T3_DstreamTransform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * 统计当前窗口内元素个数
  */
object T6_countByWindow {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(3))
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 10000)
    ssc.checkpoint("spark-demo/wincount")
    dstream
      .flatMap(_.split(" "))
      .map((_, 1))
      .countByWindow(Seconds(9),Seconds(3))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

package com.cjy.sparkstreaming.T3_DstreamTransform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 窗口滑动计算：设置窗口长度，步长
  */
object T5_WindowFunc {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(3))
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 10000)

    dstream
      .flatMap(_.split(" "))
      .map((_, 1))
      .window(Seconds(9),Seconds(3))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

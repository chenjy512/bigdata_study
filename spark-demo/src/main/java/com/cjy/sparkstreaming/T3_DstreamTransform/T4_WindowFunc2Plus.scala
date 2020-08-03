package com.cjy.sparkstreaming.T3_DstreamTransform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 窗口滑动计算：实现一T3 优化，假设每三秒计算一下最近6秒的结果，则前一个时间段需要计算两次
  *
  * t1+t2
  * t2+t3 = t1+t2+t3-t1   优化方式：加上新增的，减去旧的
  */
object T4_WindowFunc2Plus {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(3))
ssc.checkpoint("spark-demo/win")
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 10000)

    dstream
      .flatMap(_.split(" "))
      .map((_, 1))
      //
      /**
        * _+_:计算新增的
        * _-_:减去旧的
        * filterFunc：过滤统计为0的值
        */
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(9), slideDuration = Seconds(6),filterFunc = _._2 > 0)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

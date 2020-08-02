package com.cjy.sparkstreaming.T3_DstreamTransform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object T3_WindowFunc1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(3))

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 10000)

    dstream
      .flatMap(_.split(" "))
      .map((_,1))
    // 每6秒计算一次最近18秒内的wordCount
      .reduceByKeyAndWindow(_ + _, Seconds(18), slideDuration = Seconds(6))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}

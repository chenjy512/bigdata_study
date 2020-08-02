package com.cjy.sparkstreaming.T3_DstreamTransform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object T1_Transform {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")

    val sctx = new StreamingContext(conf, Seconds(3))

    val dstream: ReceiverInputDStream[String] = sctx.socketTextStream("hadoop102", 10000)

    //将dstream转为 rdd操作
    val res: DStream[(String, Int)] = dstream.transform(rdd => {
      rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    })
    res.print()

    sctx.start()
    sctx.awaitTermination()
  }
}

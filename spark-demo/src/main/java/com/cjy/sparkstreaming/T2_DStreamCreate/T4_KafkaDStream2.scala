package com.cjy.sparkstreaming.T2_DStreamCreate

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object T4_KafkaDStream2 {


  /**
    * 首次调用执行函数并创建检查点，也就是保存当时StreamingContext对象，也就保存当时消费状态，避免消费者挂掉期间，生产的数据漏消费
    * @return
    */
  def createSSC(): StreamingContext = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
    val ssc = new StreamingContext(conf, Seconds(3))

    //创建检查点
    ssc.checkpoint("spark-demo/ccl")


    val params: Map[String, String] = Map[String,String](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "group.id" -> "ccl"
    )
    //消费kafka数据
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, params, Set("ds-ccl")
    )
    //因为kafka 是 kv 数据，转换计算此时间段数据
    dstream.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    /**
      * 1。 首先从检查点ccl获取ssc 对象，如果不存在则调用createSSC 生产ssc对象
      */
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("spark-demo/ccl", createSSC)
    ssc.start() //启动
    ssc.awaitTermination()
  }
}

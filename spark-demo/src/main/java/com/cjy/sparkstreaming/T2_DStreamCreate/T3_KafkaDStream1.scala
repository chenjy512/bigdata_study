package com.cjy.sparkstreaming.T2_DStreamCreate

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object T3_KafkaDStream1 {
  def main(args: Array[String]): Unit = {

    //配置、环境对象
    val conf: SparkConf = new SparkConf().setAppName("kafka ds - 01").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //kafka 地址，消费者组
    val param: Map[String, String] = Map[String, String]("bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
      , "group.id" -> "ccl")
    //创建dstream，参数介绍：ds-ccl 被消费主题
    val dstream: InputDStream[(String, String)] = KafkaUtils.
      createDirectStream[String,String,StringDecoder,StringDecoder](ssc,param,Set("ds-ccl"))

    //计算数据
    dstream.flatMap{
      case (_,v) => v.split(" ")
    }.map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}

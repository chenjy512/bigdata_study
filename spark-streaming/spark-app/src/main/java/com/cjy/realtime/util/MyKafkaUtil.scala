package com.cjy.realtime.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {

  // kafka消费者配置
  val params= Map[String,String](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092", //用于初始化链接到集群的地址
    "group.id" -> "ccl")


  def getDStream(ssc: StreamingContext, topic: String): InputDStream[(String, String)] = {

    //消费kafka数据
   KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, params, Set(topic)
    )

  }
}
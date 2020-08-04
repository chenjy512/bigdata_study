package com.cjy.realtime

import com.cjy.realtime.bean.AdsInfo
import com.cjy.realtime.need.{AreaAdsClickTop3App, LastHourAdsClickApp}
import com.cjy.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ConsumerMockInfo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("spark-streaming/real")

    //获取kafka数据
    val dstream: InputDStream[(String, String)] = MyKafkaUtil.getDStream(ssc, "ads_log")
    val mapData: DStream[AdsInfo] = dstream.map(record => {
      val split: Array[String] = record._2.split(",")
      AdsInfo(
        split(0).toLong,
        split(1),
        split(2),
        split(3),
        split(4))
    })

    //1。求每天每地区热门广告 Top3
//    AreaAdsClickTop3App.statAreaClickTop3(mapData)

    //2。
    LastHourAdsClickApp.statLastHourAdsClick(mapData)
    ssc.start()
    ssc.awaitTermination()
  }
}

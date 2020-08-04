package com.cjy.realtime.need

import com.cjy.realtime.bean.AdsInfo
import com.cjy.realtime.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * 统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量
  */
object LastHourAdsClickApp {
  def statLastHourAdsClick(adsInfoDSteam: DStream[AdsInfo]) = {

    //1。设置滑动窗口，每5秒钟统计一次，各广告每分钟的点击量
    val window: DStream[AdsInfo] = adsInfoDSteam.window(Minutes(60),Seconds(5))

    //2。每分钟点击量，按照广告id，分钟聚合
    //注意window 滑动窗口默认统计最近一小时数据，所以这里不用 updateStateByKey 有状态函数
    val groupDS: DStream[(String, Iterable[(String, Int)])] = window
      .map(x => ((x.adsId, x.hmString), 1))
      .reduceByKey(_ + _)
      .map {
        case ((adsId, hourMinutes), count) => (adsId, (hourMinutes, count))
      }.groupByKey()

    //3。数据转为json格式
    val jsonDS: DStream[(String, String)] = groupDS.map {
      case (adsId, list) => {
        import org.json4s.JsonDSL._
        val hourMinutesJson: String = JsonMethods.compact(JsonMethods.render(list))
        (adsId, hourMinutesJson)
      }
    }

    jsonDS.print()

    //4。存入redis
    jsonDS.foreachRDD(rdd =>{

      val result: Array[(String, String)] = rdd.collect

      import collection.JavaConversions._
      val client: Jedis = RedisUtil.getClient
      client.hmset("last_hour_ads_click", result.toMap)
      client.close()
    })


  }
}

package com.cjy.realtime.need

import com.cjy.realtime.bean.AdsInfo
import com.cjy.realtime.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * 每天每地区热门广告 Top3
  */
object AreaAdsClickTop3App {

  def statAreaClickTop3(adsInfoDStream: DStream[AdsInfo]) = {

    //1。数据格式转换 ((日期，地区，广告),点击数)，并按照key聚合，求每个广告点击数
    val mapDS: DStream[((String, String, String), Int)] = adsInfoDStream.map(x => ((x.dayString,x.area,x.adsId),1))
    val reduceDs: DStream[((String, String, String), Int)] = mapDS.updateStateByKey((seq:Seq[Int],opt:Option[Int]) => Some(seq.sum+opt.getOrElse(0)))
    //2。转变数据格式（(日期，地区),(广告，点击数))，分组排序
    val groupDS: DStream[((String, String), Iterable[(String, Int)])] = reduceDs.map {
      case ((day, area, adsId), count) => ((day, area), (adsId, count))
    }.groupByKey()

    //3。求每天每地区点击前三的广告
    val top3DS: DStream[(String, String, List[(String, Int)])] = groupDS.map {
      case ((day, area), list) => {
        val top3: List[(String, Int)] = list.toList.sortBy(-_._2).take(3);
        (day, area, top3)
      }
    }

    //
    top3DS.print()

    //4。存入redis数据
    top3DS.foreachRDD(rdd =>{
      //获取redis客户端
      val client: Jedis = RedisUtil.getClient
      //由于是时时数据，所以不多收集到driver中计算
      val arr: Array[(String, String, List[(String, Int)])] = rdd.collect()
      println(arr.toList + "------>")
      //循环更新数据
        arr.foreach{
          case (day, area, adsIdCountList) => {
            import org.json4s.JsonDSL._
            // list 集合转成 json 字符串
            val adsCountJsonString = JsonMethods.compact(JsonMethods.render(adsIdCountList))
            client.hset(s"area:day:top3:$day", area, adsCountJsonString)
          }
      }
      // 连接放回连接池中
      client.close()
    })
  }
}

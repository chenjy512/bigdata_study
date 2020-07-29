package com.cjy.sparkcore.T11_project.need2

import com.cjy.sparkcore.T11_project.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 需求二：Top10热门品类中每个品类的 Top10 活跃 Session 统计
  */
object CategorySessionApp {

  /**
    *需求实现方式一：缺点，将品类id的数据转为list加载进内存排序可能会造成 OOM异常
    * @param sc                 上下文对象
    * @param userVisitActionRDD 数据全集
    * @param categoryTop10      热门前十品类
    */
  def queryCategorySessionTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction],
                                categoryTop10: List[CategoryCountInfo]):  RDD[CategorySession]  = {
    //1. 热门品类id 前十
    val cids: List[String] = categoryTop10.map(x => x.categoryId)
    //2. 过滤，取出热门id数据
    val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(x => cids.contains(x.click_category_id + ""))
    //3. 转换格式
    val mapRDD: RDD[((Long, String), Int)] = filterRDD.map(x => ((x.click_category_id, x.session_id), 1))
    //4. 聚合，求每个品类每个用户的点击次数
    val reduceRDD: RDD[((Long, String), Int)] = mapRDD.reduceByKey(_ + _)
    //    reduceRDD.foreach(println)
    //5. 转换数据格式：((cid,sid),num) => (cid,(sid,num)),便于按照cid分组
    val mapRDD2: RDD[(Long, (String, Int))] = reduceRDD.map(x => (x._1._1, (x._1._2, x._2)))
    //6. 按照品id 分组
    val groupRDD: RDD[(Long, Iterable[(String, Int)])] = mapRDD2.groupByKey()

    //7. 点击前十sesionid
    //方式一
    val sessions: List[CategorySession] = sessionClickTop10(groupRDD)
    sessions.foreach(println(_))

    //方式二：直接扁平化处理
    val sessionTop10: RDD[CategorySession] = groupRDD.flatMap {
      case (cid, its) => {
        //点击session 点击次数排序取前十
        //注意：toList将RDD转为了scala 的list，所以全部数据会加载进内存中，有可能内存溢出
        val top10: List[(String, Int)] = its.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
        //转变数据封装格式
        top10.map {
          case (sid, num) => CategorySession(cid.toString, sid, num)
        }
      }
    }
//    sessionTop10.foreach(println(_))
    sessionTop10
  }

  //转换方式一：使用map求每个cid的前十点击session结果再扁平化
  def sessionClickTop10(groupRDD: RDD[(Long, Iterable[(String, Int)])]): List[CategorySession] = {
    //1 求每个cid的前十session
    val sesionTop10: RDD[List[CategorySession]] = groupRDD.map {
      case (cid, its) => {
        val sesionTop10: List[(String, Int)] = its.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
        val sessions: List[CategorySession] = sesionTop10.map {
          case (sid, num) => CategorySession(cid.toString, sid, num)
        }
        sessions
      }
    }
    //2 每个cid是一个集合，所以扁平化
    sesionTop10.flatMap(x => x).collect().toList
  }
}

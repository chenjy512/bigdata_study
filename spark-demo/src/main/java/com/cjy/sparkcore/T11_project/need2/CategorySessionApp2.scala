package com.cjy.sparkcore.T11_project.need2

import com.cjy.sparkcore.T11_project.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * 需求二：Top10热门品类中每个品类的 Top10 活跃 Session 统计
  */
object CategorySessionApp2 {

  /**
    *需求实现方式二：
    *             解决内存溢出，但是提交job任务过多
    * @param sc                 上下文对象
    * @param userVisitActionRDD 数据全集
    * @param categoryTop10      热门前十品类
    */
  def queryCategorySessionTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction],
                                categoryTop10: List[CategoryCountInfo]): RDD[CategorySession] = {
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
    val arr: ArrayBuffer[CategorySession] = ArrayBuffer[CategorySession]()
    cids.foreach(x =>{
      //take 行动算子，触发job
      val top10: Array[(Long, (String, Int))] = mapRDD2.filter(_._1 == x.toLong).sortBy(_._2._2,ascending = false).take(10)
      val sessions: Array[CategorySession] = top10.map {
        case (cid, its) => CategorySession(cid.toString, its._1, its._2)
      }
//      sessions.foreach(println(_))
      arr.appendAll(sessions)
    })
    arr.foreach(println)
    val mk2: RDD[CategorySession] = sc.makeRDD(arr)
    mk2
  }


}

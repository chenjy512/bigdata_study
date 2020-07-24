package com.cjy.sparkcore.T11_project

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 需求实现函数
  */
object LogNeedsApp {


  /**
    * 需求一： Top10 热门品类，按照点击次数优先
    *
    * @param sc
    * @param mapRDD
    * @return
    */
  def dealNeedCategorTop10(sc: SparkContext, mapRDD: RDD[UserVisitAction]): List[CategoryCountInfo] = {
    //1. 注册累加器
    val acc = new LogAccumulator
    sc.register(acc, "LogAccumulator")

    //2. 循环遍历触发行动算子，针对三种情况数据进行处理，将数据追加进累加器处理
    mapRDD.foreach(userVisitAction => {
      if (userVisitAction.click_category_id != -1) {
        acc.add((userVisitAction.click_category_id.toString, "click"))
      } else if (userVisitAction.order_category_ids != "null") {
        val ids: Array[String] = userVisitAction.order_category_ids.split(",")
        ids.foreach(oid => acc.add((oid, "order")))
      } else if (userVisitAction.pay_category_ids != "null") {
        userVisitAction.pay_category_ids.split(",").foreach(pid => acc.add((pid, "pay")))
      }
    })
    //4. 行为数据集
    val datasMap: mutable.Map[(String, String), Long] = acc.value

    //5. 根据品类id进行分组
    val sortMap: Map[String, mutable.Map[(String, String), Long]] = datasMap.groupBy(x => x._1._1)

    //6. 装换数据格式
    val categoryCountInfoList: List[CategoryCountInfo] = sortMap.map {
      case (cid, acctionMap) => CategoryCountInfo(cid, acctionMap.getOrElse((cid, "click"), 0)
        , acctionMap.getOrElse((cid, "order"), 0)
        , acctionMap.getOrElse((cid, "pay"), 0))
    }.toList

    //7. 按照订单、支付、点击次数先后排序
    val infoes: List[CategoryCountInfo] = categoryCountInfoList.sortBy(
      info => (info.clickCount, info.orderCount, info.payCount))
    (Ordering.Tuple3(Ordering.Long.reverse, Ordering.Long.reverse, Ordering.Long.reverse))

    //8. 取品类前十
    val topData: List[CategoryCountInfo] = infoes.take(10)
    topData
  }


}

package com.cjy.sparkcore.T11_project

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 累加器：入参（品类，类型）
  * 返回值 Map[(String,String),Long]: Map[（品类，类型），次数]
  */
class LogAccumulator extends AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] {
  //返回值数据容器
  val dataMap = mutable.Map[(String, String), Long]()

  //判断是否数据为空
  override def isZero: Boolean = dataMap.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] = {
    val newAcc = new LogAccumulator
    dataMap.synchronized {
      newAcc.dataMap ++= dataMap
    }
    newAcc
  }

  //累加器初始化
  override def reset(): Unit = dataMap.clear()

  //累加数据
  override def add(v: (String, String)): Unit = {
    //数据累加
    dataMap(v) = dataMap.getOrElse(v, 0L) + 1L
  }

  /**
    * 合并多个task之间的累加器数据容器
    * @param other
    */
  override def merge(other: AccumulatorV2[(String, String), mutable.Map[(String, String), Long]]): Unit = {
    val datas: mutable.Map[(String, String), Long] = other.value
    datas.foreach {
      kv => {
        //当合并task时，dataMap中可能没有datas中的key，所以不能使用赋值，需要使用put写入或覆盖数据
        //      dataMap(kv._1) = dataMap.getOrElse(kv._1,0)+kv._2
        dataMap.put(kv._1, dataMap.getOrElse(kv._1, 0L) + kv._2)
      }
    }
  }

  //返回累加器值
  override def value: mutable.Map[(String, String), Long] = {
    dataMap
  }
}

package com.cjy.sparkSQL.T4_project

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * 自定义聚合函数：统计分组后每个城市占比 --->  北京21.2%，天津13.2%，其他65.6%
  */
class AreaClickUDAF extends UserDefinedAggregateFunction {

  //输入数据类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("cityName", StringType)))
  }

  /**
    * 初始化缓冲区中数据类型: 统计每个城市出现次数，统计总次数
    */
  override def bufferSchema: StructType = {
    StructType(Array(StructField("map", MapType(StringType, LongType)), StructField("count", LongType)))
  }

  //最终数据返回值类型
  override def dataType: DataType = StringType

  //确定性
  override def deterministic: Boolean = true

  /**
    * 初始化缓冲区
    *
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  /**
    * 计算分区内数据
    *
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match {
      case Row(cityName: String) => {
        //1. 分区内总数➕ 1
        buffer(1) = buffer.getLong(1) + 1
        //2. 当前城市 ➕ 1
        val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
      }
      case _ =>
    }
  }

  /**
    * 分区内数据合并
    * @param buffer1  返回的缓冲，所以合并到此分区
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //1. 计算总数
    val count = buffer1.getLong(1) + buffer2.getLong(1)
    buffer1(1) = count
    //2. 合并城市出现次数
    val map1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
    // 将map1中的数据便利处理，初始化数据是map2，每次map2与map1中的一个元素合并后返回和名的map继续处理下一个元素
    val resMap: collection.Map[String, Long] = map1.foldLeft(map2) {
      case (map, (cityName, count)) =>
        map + (cityName -> (map.getOrElse(cityName, 0L) + count))
    }
    buffer1(0) = resMap
  }

  /**
    * 处理最终返回类型
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    //1. 获取出现总次数
    val count: Long = buffer.getLong(1)
    //2. 获取map数据
    val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    //3. 点击次数倒排序取前两名
    val top2: List[(String, Long)] = map.toList.sortBy(-_._2).take(2)

    var mapList: List[CityRemark] = top2.map {
      case (x,y) => CityRemark(x, y.toDouble/count)
    }
    //大于二，计算其它
    if(map.size > 2){

//      val countTop2: Double = mapList.foldLeft(0d)(_ + _.cityRedio)
//      mapList = mapList :+ CityRemark("其它",1L - countTop2)

      //foldLeft 巧用方式
      mapList = mapList :+ CityRemark("其它",mapList.foldLeft(1D)(_-_.cityRedio))
    }
    //返回结果
    mapList.mkString(",")
  }
}
case class CityRemark(name:String,cityRedio:Double){

//  val f = new DecimalFormat("0.00%")
  val f = new DecimalFormat("0%")

  override def toString: String = {
    s"$name:${f.format(cityRedio.abs)}"
  }
}

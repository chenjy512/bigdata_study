package com.cjy.sparkcore.T11_project.need2

import org.apache.spark.Partitioner

class MyPartition(cids:List[String]) extends Partitioner{
  //转变格式，根据坐标确定分区
  private val map: Map[String, Int] = cids.zipWithIndex.toMap
  override def numPartitions: Int = map.size
  //根据key取分区号
  override def getPartition(key: Any): Int = {
    key match {
      case (id:Long,_)=>map(id.toString)
    }
  }
}

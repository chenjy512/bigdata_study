package com.cjy.spark.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupByKey：groupByKey也是对每个key进行操作，将相同key的value聚合到一个seq中，但最后只生成一个sequence。
  */
object T20_OperGroupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("double-value").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD: RDD[String] = sc.makeRDD(Array("one", "two", "two", "three", "three", "three"))
    //数据转为 k-v 类型
    val mapRDD: RDD[(String, Int)] = mkRDD.map((_,1))
    //将key相同元素聚集到同一sequence 中
    val groupBykeyRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    //查看相同key聚集到一个kv中
    groupBykeyRDD.collect().foreach(println(_))
    //统计相同key个数
    //方式一：直接调用value的sum函数
    val countRDD: RDD[(String, Int)] = groupBykeyRDD.map(x => (x._1,x._2.sum))
    //方式二：遍历元素针对value序列reduce求和
    val countRDD2: RDD[(String, Int)] = groupBykeyRDD.map(x => (x._1,x._2.reduce(_+_)))
    countRDD.collect().foreach(println)
    countRDD2.collect().foreach(println)
  }
}

package com.cjy.spark.T10_accumulator

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

/**
  * 自定义累加器
  *   1.String 添加元素类型
  *   2. java.util.ArrayList[String] 累加器值，也就是容器类型
  */
class WordAccumulator extends AccumulatorV2[String,java.util.ArrayList[String]]{
  val list = new util.ArrayList[String]();
  //是否为初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }
  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator
  }
  //重置
  override def reset(): Unit = list.clear()
  //添加元素
  override def add(v: String): Unit = {
    if(v.contains("h")){
      list.add(v)
    }
  }
  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
      list.addAll(other.value)
  }
  //返回
  override def value: util.ArrayList[String] = list
}

object WordAccumulator{
  def main(args: Array[String]): Unit = {
    //1. 环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")
    val sc = new SparkContext(sparkConf)
    //2. 初始化数据
    val mkRDD: RDD[String] = sc.makeRDD(List("hive","spark","scala","hbase","java","hadoop"),2)
    //3. 创建并注册累加器
    val accumulator = new WordAccumulator
    sc.register(accumulator)
    //4. 遍历数据
    mkRDD.foreach{
      case str =>{
        //执行累加器操作
        accumulator.add(str)
      }
    }
    //5. 获取累加器数据
    println(accumulator.value)
  }
}
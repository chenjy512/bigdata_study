package com.cjy.sparkcore.T10_accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 系统累加器练习
  */
object LogAccumulatorTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")
    val sc = new SparkContext(sparkConf)
    //创建累加器对象
    val lc: LongAccumulator = sc.longAccumulator
    val mkRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10),2)

    mkRDD.map(x => {
      if(x%2==0){
          //执行累加器操作
          lc.add(1)
      }
      x
    }).collect()
    //打印累加器值
    println(lc.value)
  }
}

package com.cjy.spark.T2_oneValue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * sample：以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，
  *         true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。
  */
object T9_OperSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD-Create").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val mkRDD = sc.makeRDD(1 to 12)
    //抽样
    val sample = mkRDD.sample(true,0.4,1)
    sample.collect().foreach(println)
  }
}

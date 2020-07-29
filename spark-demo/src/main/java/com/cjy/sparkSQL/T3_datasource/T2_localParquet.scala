package com.cjy.sparkSQL.T3_datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 加载默认格式parquet 文件
  */
object T2_localParquet {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境对象
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("load data ")
      .getOrCreate()

    val df: DataFrame = spark.read.parquet("spark-demo/write/1.parquet")

    df.show

    spark.stop()
  }
}

package com.cjy.sparkSQL.T3_datasource

import com.cjy.sparkSQL.T2_func.MyAvg
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * spark.read.load 是加载数据的通用方法.
  * df.write.save 是保存数据的通用方法.
  *
  * 注意：加载文件可是使用指定函数如json，parquet，也可以是用format来指定格式
  *      写出数据: 1. 默认是parquet格式，可是选择其它模式
  *               2. 写出数据路径存在默认保存，可以指定覆盖、追加等模式
  */
object T1_localJson {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境对象
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("load data ")
      .getOrCreate()

    val df: DataFrame = spark.read.json("spark-demo/in/user.json")
    df.show
//    df.select("age").write.save("spark-demo/write/1.parquet")
    //覆盖
//    df.select("age").write.mode(SaveMode.Overwrite).save("spark-demo/write/1.parquet")
    df.select("age").write.format("json").mode(SaveMode.Overwrite).save("spark-demo/write/1.parquet")
    //指定加在数据格式
    val df2: DataFrame = spark.read.format("json").load("spark-demo/in/user.json")
    df2.show()
    spark.stop()
  }
}

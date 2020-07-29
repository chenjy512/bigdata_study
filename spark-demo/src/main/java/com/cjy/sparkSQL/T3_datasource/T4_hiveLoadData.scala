package com.cjy.sparkSQL.T3_datasource

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object T4_hiveLoadData {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .enableHiveSupport()    //启动hive支持
      .getOrCreate()

    import spark.implicits._
    import spark.sql

//    sql("show databases").show
    val df: DataFrame = sql("show databases")
    df.show()
    //hive 计算结果保存，能保存到本地，也就能保存到mysql 等。。。
    df.write.format("json").mode(SaveMode.Overwrite).save("spark-demo/hive")
    spark.close()
  }
}

package com.cjy.sparkSQL.T3_datasource

import org.apache.spark.sql.SparkSession

object T4_hiveLoadData {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("show databases").show

    spark.close()
  }
}

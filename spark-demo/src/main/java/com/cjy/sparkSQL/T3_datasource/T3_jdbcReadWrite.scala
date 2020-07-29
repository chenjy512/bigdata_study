package com.cjy.sparkSQL.T3_datasource

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object T3_jdbcread {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    read(spark)
//    insert(spark)
    spark.stop()
  }

  def read(spark: SparkSession ): Unit ={
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/hymc")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "admin") //读取的表
      .load()
    jdbcDF.show
  }

  def insert(spark: SparkSession )={

    import spark.implicits._
    val rdd: RDD[Admin] = spark.sparkContext.parallelize(Array(Admin(5,"aaaa"), Admin(6, "bbbb")))
    val ds: Dataset[Admin] = rdd.toDS

    //方式一：
//    ds.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://hadoop102:3306/hymc")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "admin")
//      .mode(SaveMode.Append)  //追加方式
//      .save()

    //方式二：
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop102:3306/hymc", "admin", props)
  }

}

case  class Admin(id:Int,password:String)
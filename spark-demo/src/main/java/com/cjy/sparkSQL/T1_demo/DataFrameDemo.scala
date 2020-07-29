package com.cjy.sparkSQL.T1_demo

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * sparksql 操作本地文件
  */
object DataFrameDemo {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sql-demo").getOrCreate()

    val df: DataFrame = spark.read.json("spark-demo/in/user.json")
    df.show()
    //使用 DSL 语法 导入如下隐式转换类，并且spark.implicits._ 中的spark 必须与 如上spark: SparkSession 对象名一致
    import spark.implicits._
    df.filter($"age">20).show
    //创建临时视图对象
    df.createTempView("person")
    spark.sql("select * from person t where t.name='lisi'").show()
    spark.stop()
  }
}

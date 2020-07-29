package com.cjy.sparkSQL.T1_demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * sparksql 操作三种数据类型转换
  */
object TransformDataDemo {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sql-demo").getOrCreate()


    import spark.implicits._

    //1. rdd to df
    val rdd = spark.sparkContext.makeRDD(Array(("zhangsan",12),("list",21),("wangwu",16)))
    val df1: DataFrame = rdd.toDF("name","age")
    df1.show()

    //2. rdd to ds
    val ds: Dataset[(String, Int)] = rdd.toDS()
    ds.show()
    val rdd2 = spark.sparkContext.makeRDD(Array(Student("zhangsan",12),Student("list",21),Student("wangwu",16)))
    rdd2.toDS().show()

    //3. df to ds
    val ds2: Dataset[Student] = df1.as[Student]
    ds2.show()

    //4. df to rdd
    val rdd3: RDD[Row] = df1.rdd
    rdd3.collect().foreach(println)

    //5. ds to df
    ds2.toDF().show()

    //6. ds to rdd ，封装类型就换成类型的
    ds2.rdd.collect().foreach(println)

    spark.stop()
  }
}

case class Student(name:String,age:Int)
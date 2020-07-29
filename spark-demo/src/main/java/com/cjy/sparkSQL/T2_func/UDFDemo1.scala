package com.cjy.sparkSQL.T2_func

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object UDFDemo1 {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境对象
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("UDFDemo1")
      .getOrCreate()
    // 2. 导入隐士转换类，注册自定义函数
    import spark.implicits._
    //myAvg：函数名
    spark.udf.register("myAvg",new MyAvg)
    //3. 加载数据，创建试图
    val df: DataFrame = spark.read.json("spark-demo/in/user.json")
    df.show
    df.createTempView("user")
    //4. 使用自定义函数
    spark.sql("select myAvg(age) from user").show
    spark.stop()
  }
}

/**
  * 基础自定义函数 抽象类，实现其方法
  * 自定义求平均值函数
  */
class MyAvg extends UserDefinedAggregateFunction {

  /**
    * 返回聚合函数输入参数的数据类型
    *
    * @return
    */
  override def inputSchema: StructType = {
    StructType(StructField("inputColumn", DoubleType) :: Nil)
  }

  /**
    * 聚合函数缓冲区中的数据类型
    *
    * @return
    */
  override def bufferSchema: StructType = {
    StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)
  }

  /**
    * 最终返回值的类型
    *
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    * 确定性：比如同样的输入是否返回同样的输出
    *
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 初始化-与缓冲区中定义数据位置相同
    *
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0d //总和
    buffer(1) = 0L //数据个数
  }

  /**
    * 分区内聚合
    *
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //判断当前行传入参数是否为空
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  /**
    * 分区间聚合
    *
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (!buffer2.isNullAt(0)) {
      //区间数据合并
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
  }

  /**
    * 最终计算后的返回值
    *
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Double = {
    println(buffer.getDouble(0), buffer.getLong(1))
    buffer.getDouble(0) / buffer.getLong(1)
  }
}
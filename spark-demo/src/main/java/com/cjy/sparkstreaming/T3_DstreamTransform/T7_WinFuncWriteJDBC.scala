package com.cjy.sparkstreaming.T3_DstreamTransform

import java.util.Properties

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 窗口滑动计算：将汇总结果写入到mysql中
  */
object T7_WinFuncWriteJDBC {
  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(3))

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 10000)
ssc.checkpoint("spark-demo/winjdbc")
    dstream
      .flatMap(_.split(" "))
      .map((_,1))
      .filter(!_._1.isEmpty)
      .updateStateByKey((seq:Seq[Int],opt:Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
      .foreachRDD(rdd =>{
        // 1. 先创建sparkSession
        val spark = SparkSession.builder()
          .config(rdd.sparkContext.getConf)
          .getOrCreate()

        import spark.implicits._
        val df: DataFrame = rdd.toDF("word","count")
        df.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoop102:3306/test","win_word",prop)
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

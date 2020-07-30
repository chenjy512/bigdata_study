package com.cjy.sparkstreaming.T1_SocketDemo

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    //1. 创建配置及环境对象
    val conf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))  //设置间隔时间
    //2. 创建 socket DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    //3。 获取一个个数据
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //4。 数据格式转换
    val mapWords: DStream[(String, Int)] = words.map((_,1))
    //5。 数据聚合
    val reduce: DStream[(String, Int)] = mapWords.reduceByKey(_+_)
    println("-----------")
    //6。 数据打印
    reduce.print()
    //7。 开始接收数据并计算
    ssc.start()
    //8。 等待计算结束(要么手动退出,要么出现异常)才退出主程序
    ssc.awaitTermination()
  }
}

package com.cjy.sparkstreaming.T3_DstreamTransform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 有状态处理：将当前时间段数据聚合到之前全部数据中，更新全部数据
  */
object T2_UpstateByKeyDemo1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint("spark-demo/update")
//    ssc.sparkContext.setCheckpointDir("spark-demo/update")
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 10000)

    val ms: DStream[(String, Int)] = dstream.flatMap(_.split(" ")).map((_,1))

    //seq：当前时间段内的数据统计，opt：以前的数据统计和

    //方式一
//    ms.updateStateByKey((seq:Seq[Int],opt:Option[Int]) =>{
////      println(seq.sum +"---"+ opt.getOrElse(0))
//      Some(seq.sum + opt.getOrElse(0))
//    }).print()

    //方式二：
    ms.updateStateByKey(updateStataCount).print()
    def updateStataCount(newData:Seq[Int],runData:Option[Int]) ={
      // 新的总数和状态进行求和操作
      val i = newData.sum+runData.getOrElse(0)
      Some(i)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

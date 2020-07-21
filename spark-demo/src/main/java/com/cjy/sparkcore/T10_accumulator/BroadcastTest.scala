package com.cjy.sparkcore.T10_accumulator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量：只读类型，用于数据spark调优策略
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")
    val sc = new SparkContext(sparkConf)
    //两个rdd join操作
    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c")))
/*    val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,2),(3,3)))
    val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2)
    joinRDD.foreach(println)*/
    //创建 广播变量
    val list = List((1,1),(2,2),(3,3))
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    //使用广播变量减少数据传输
    val resRDD: RDD[(Int, (String, Any))] = rdd1.map {
      case (k, v) => {
        var v2: Any = null
        for (t <- broadcast.value) {
          //判断k相等
          if (k == t._1) {
            v2 = t._2
          }
        }
        //合并value值
        (k, (v, v2))
      }
    }
    resRDD.foreach(println(_))
  }
}

package com.cjy.spark.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregateByKey：针对区内，区间不同操作
  */
object T23_OperFoldByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aggregate").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val mkRDD: RDD[(String,Int)] = sc.makeRDD(Array(("two",12),("three",3), ("two",2), ("two",3), ("three",4), ("three",3)),2)
    //查看分区数据
    mkRDD.glom().collect().foreach(x => println( x.mkString(",")))
    /**参数解读：aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]
      * zeroValue：初始化值，假设某个key只有一个数据，则无法完成分区内最大值，分区间求和，所以需要初始值
      * seqOp：区内内比最大值，u是初始值或上次比较值，v是按照key分组后的value值，推导都是int型所以直接操作
      * combOp：区间求和，每个区比较最大值后每个区的key只有一个数据，所以两区间求和
      */
      //完整编写
    val a2: RDD[(String, Int)] = mkRDD.aggregateByKey(0)((x,y) => math.max(x,y),(x,y)=>x+y)
      //类型推导
    val aggeraget: RDD[(String, Int)] = mkRDD.aggregateByKey(0)(Math.max(_,_),_+_)
    aggeraget.collect().foreach(println)
    a2.collect().foreach(println)

  }
}

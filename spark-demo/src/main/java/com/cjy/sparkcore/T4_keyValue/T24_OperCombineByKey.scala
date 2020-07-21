package com.cjy.sparkcore.T4_keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** 参数解读
  * combineByKey[C](createCombiner: V => C,mergeValue: (C, V) => C,mergeCombiners: (C, C) => C): RDD[(K, C)]
  *
  * createCombiner: V => C 将value转变数据类型，与下一个value传入分区计算函数中，简单说就是改变value类型
  * mergeValue: (C, V) => C 分区内计算，并返回转变后的类型
  * mergeCombiners: (C, C) => C 分区间计算并返回转变后的类型
  *
  */
object T24_OperCombineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aggregate").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val mkRDD: RDD[(String,Int)] = sc.makeRDD(Array(("two",12),("two",4),("three",3), ("two",2), ("two",3), ("three",4), ("three",3)),2)
    //查看分区数据
    mkRDD.glom().collect().foreach(x => println( x.mkString(",")))

    //完整参数
    val comRDD: RDD[(String, (Int, Int))] = mkRDD.combineByKey(x=>(x,1),(tp:(Int,Int),v:Int)=> (tp._1+v,tp._2+1),(x:(Int,Int),y:(Int,Int))=>(x._1+y._1,x._2+y._2))

    //定义rdd时已知v是int行，所以使用时会自动推导-差别不大
    mkRDD.combineByKey((_,1),(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    comRDD.collect().foreach(println(_))
    val avgRDD: RDD[(String, Double)] = comRDD.map(x=>(x._1,x._2._1/x._2._2.toDouble))
    avgRDD.collect().foreach(println)
  }
}

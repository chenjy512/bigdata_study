package com.cjy.sparkcore.T6_funTransmit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 函数传递：传递一个函数，实际上调用者是当前对象，所以类需要序列化
  *
  * 属性传递：如下在匿名函数中传递的是已经赋值给str的字符串已经与对象本身无关，所以在发送给executor时其实传送的是str字符，
  *           而String本身已经实现了序列化接口
  */
object SeriTest {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("SeriTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))
    val search: Search = new Search("h")
    val getRDD: RDD[String] = search.getMatch(rdd)
    getRDD.foreach(println(_))

    val search2: Search2 = new Search2("h")
    val getRDD2: RDD[String] = search2.getMatch2(rdd)
    getRDD2.collect().foreach(println(_))
  }
}

class Search(s:String) extends Serializable {
  //判断是否包含s字符
  def isMatch(query:String): Boolean ={
    query.contains(s)
  }
  //过滤，注意isMatch函数调用其实是，this.isMatch，所以用到了Search对象，所以Search类需要序列化
  def getMatch(rdd:RDD[String]):RDD[String]={
        rdd.filter(isMatch(_))
  }
}

//属性传递
class Search2(s:String) {

  def getMatch2(rdd:RDD[String]):RDD[String]={
    //1. 获取s值
    val str = s
    //传入匿名函数，与str值
    rdd.filter(x => x.contains(str))
  }
}
package com.cjy.spark.T9_saveData

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * 操作json文件：如果JSON文件中每一行就是一个JSON记录，那么可以通过将JSON文件当做文本文件来读取，然后利用相关的JSON库对每一条数据进行JSON解析。
  *     textFile：读取--map转换类型
  *     saveAsTextFile：保存
  *
  *     数据格式
{"name": "zhangsan","age": 18}
{"name": "lisi","age": 21}
{"name": "wangwu","age": 22}
  */
object T2_OperJsonFile {
  def main(args: Array[String]): Unit = {
    //1.配置
    val conf = new SparkConf().setAppName("wc").setMaster("local[1]")
    //2.创建上下文对象
    val sc = new SparkContext(conf)
    //3 读取目录
    val jsonRDD: RDD[String] = sc.textFile("spark-demo/in/user.json")
    //4 转变json格式
    val result: RDD[Option[Any]] = jsonRDD.map(JSON.parseFull)
    val collArr: Array[Option[Any]] = result.collect()
    collArr.foreach(x => println(x))

    //5 获取内容
    val value: RDD[List[Any]] = result.map(x => x.toList.map(x => x))
    val array: Array[List[Any]] = value.collect()

    val list: List[Any] = parse(array)
    println(list)

    for (elem <- list) {
      val map: Map[String, Any] = elem.asInstanceOf[Map[String, Any]]
      println(map.get("name").get+"--"+ map.get("age").get)
    }
    println("---------")

    // json数据保存到本地
    val mkRDD: RDD[String] = sc.makeRDD(Array("{\"name\": \"zhangsan\",\"age\": 18}","{\"name\": \"lisi\",\"age\": 21}","{\"name\": \"wangwu\",\"age\": 22}"),1)
    mkRDD.saveAsTextFile("spark-demo/json")



  }
  def parse(arr:Array[List[Any]]):List[Any]={
    var list: List[Any] = List.empty
    arr.foreach(x =>{
      for(map <- x){
        list = list :+ map
      }
    })
    list
  }
}

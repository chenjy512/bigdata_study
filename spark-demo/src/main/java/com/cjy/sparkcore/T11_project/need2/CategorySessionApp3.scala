package com.cjy.sparkcore.T11_project.need2

import com.cjy.sparkcore.T11_project.{CategoryCountInfo, CategorySession, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 需求二：Top10热门品类中每个品类的 Top10 活跃 Session 统计
  */
object CategorySessionApp3 {

  /**
    *需求实现方式二：
    *             解决内存溢出，但是提交job任务过多
    * @param sc                 上下文对象
    * @param userVisitActionRDD 数据全集
    * @param categoryTop10      热门前十品类
    */
  def queryCategorySessionTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction],
                                categoryTop10: List[CategoryCountInfo]): RDD[CategorySession] = {
    //1. 热门品类id 前十
    val cids: List[String] = categoryTop10.map(x => x.categoryId)
    //2. 过滤，取出热门id数据
    val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(x => cids.contains(x.click_category_id + ""))
    //3. 转换格式
    val mapRDD: RDD[((Long, String), Int)] = filterRDD.map(x => ((x.click_category_id, x.session_id), 1))
    //4. 使用自定义分区器-将相同cid预聚合，计算每个分区值
    val reduceRDD: RDD[((Long, String), Int)] = mapRDD.reduceByKey(new MyPartition(cids),_+_)
    //5. 二次转变数据格式
    val mapRDD2: RDD[CategorySession] = reduceRDD.map(x => CategorySession(x._1._1.toString,x._1._2,x._2))
    //6. mapPartitions按照每个分区数据执行，一个分区数据集是一个元素,如下mapPartitions会执行10次，但是只会提交一个job
    val mpRDD: RDD[CategorySession] = mapRDD2.mapPartitions(x => {
      //前十容器，CategorySession需要继承比较类，重写比较规则；
      var treeSet: mutable.TreeSet[CategorySession] = mutable.TreeSet[CategorySession]()
      //处理每个分区数据
      x.foreach(cs => {
        treeSet += cs
        if (treeSet.size > 10) {
//          treeSet默认倒排序，所以取前十个
          treeSet = treeSet.take(10)
        }
      })
      treeSet.toIterator
    })
      //行动算子触发job
//    mpRDD.collect().foreach(println)
    mpRDD
  }


}

package com.cjy.sparkcore.T11_project.need3

import java.text.DecimalFormat

import com.cjy.sparkcore.T11_project.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 3: 页面单跳转化率统计
  * 计算页面单跳转化率，什么是页面单跳转换率，比如一个用户在一次 Session 过程中访问的页面路径 3,5,7,9,10,21，
  * 那么页面 3 跳到页面 5 叫一次单跳，7-9 也叫一次单跳，那么单跳转化率就是要统计页面点击的概率
      比如：计算 3-5 的单跳转化率，先获取符合条件的 Session 对于页面 3 的访问次数（PV）为 A，
      然后获取符合条件的 Session 中访问了页面 3 又紧接着访问了页面 5 的次数为 B，那么 B/A 就是 3-5 的页面单跳转化率.

  思路：session跳转路径，每个session用户按照访问时间顺序排序，然后分为两个集合错位一个元素zip合并，每个元素就是单次跳转。
        切记session访问页面不能进行过滤，不然会丢失准确度，例如访问顺序 1-3-2，过滤3，则变成1-2；
        单次跳转路径zip错位组合
  */
object PageConversionApp {
  def calcPageConversion(spark: SparkContext, userVisitActionRDD: RDD[UserVisitAction], targetPageFlow: String) = {
    //1. 跳转路径处理
    //获取页面id
    val targetARR: Array[String] = targetPageFlow.split(",")
    //组合跳转路径
    val pre: Array[String] = targetARR.slice(0,targetARR.length-1)
    val post: Array[String] = targetARR.slice(1,targetARR.length)
    val targetTuple: Array[(String, String)] = pre.zip(post)
    val targetPage: Array[String] = targetTuple.map(x => x._1+"_"+x._2)

    //2. 计算每个页面访问次数：过滤符合条件页面，转换数据格式，分组求和
    val countByPageId: collection.Map[Long, Long] = userVisitActionRDD.filter(x => targetARR.contains(x.page_id.toString)).map(x => (x.page_id,1L)).countByKey()

    //3. 计算每个session的跳转过程
    //3.1 按照sessionid分组
    val groupSession: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)
    //3.2 计算每个session的访问路径
    val sessionPaths: RDD[String] = groupSession.flatMap {
      case (sid, its) => {
        //每个session按照访问时间排序
        val actions: List[UserVisitAction] = its.toList.sortBy(_.action_time)
        //计算访问路径
        val preList: List[UserVisitAction] = actions.slice(0, actions.size - 1)
        val postList: List[UserVisitAction] = actions.slice(1, actions.size)
        val zipList: List[(UserVisitAction, UserVisitAction)] = preList.zip(postList)
        val paths: List[String] = zipList.map(x => x._1.page_id + "_" + x._2.page_id)
        //过滤不符合条件的跳转路径
        paths.filter(targetPage.contains(_))
      }
    }
    //4. 聚合每个单跳路径次数
    val sessionPathsCount: Array[(String, Int)] = sessionPaths.map(x =>(x,1)).reduceByKey(_+_).collect()
    val sortSessionPathCount: Array[(String, Int)] = sessionPathsCount.sortBy(x => x._1)
    //5. 计算跳转率
    val formatter = new DecimalFormat(".00%")
    val tuples: Array[(String, String)] = sortSessionPathCount.map {
      case (path, num) => {
        //获取跳转前页面访问总数
        val firstPathCount = countByPageId.getOrElse(path.split("_").head.toLong, 0L)
        //计算单次跳转率
        val str: String = formatter.format(num.toDouble / firstPathCount)
        (path, str)
      }
    }
    tuples.foreach(println)
    tuples
  }
}

package com.cjy.sparkSQL.T4_project

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AreaApp{
  def main(args: Array[String]): Unit = {

    //1. 创建环境对象
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .enableHiveSupport()    //启动hive支持
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    //2. 注册聚合函数
    spark.udf.register("cityRemark",new AreaClickUDAF)
    sql("use ccl")

    //3. 分组聚合
    sql(
      """
        |select c.area,p.product_name name ,count(*) total,cityRemark(city_name) remark
        |from user_visit_action u left join product_info p on p.product_id=u.click_product_id
        |left join city_info c on c.city_id=u.city_id
        |where u.click_product_id>0
        |group by c.area,p.product_name
      """.stripMargin).createTempView("t1")

    //4. 开窗排序，取前三
    val df: DataFrame = sql(
      """
select t2.*
from (
select t1.area,t1.name,total,row_number() over(partition by t1.area order by total desc ) num , remark
from t1
) t2 where t2.num < 4
      """.stripMargin)

    df.show()

//    df.write.mode(SaveMode.Overwrite).save()  ....  可将结果保存下来
    spark.close()
  }
}

/**
  * |area| name|total|num|              remark|
  * +----+-----+-----+---+--------------------+
  * |  华东|商品_86|  371|  1|上海:16%,无锡:16%,其它:68%|
  * |  华东|商品_47|  366|  2|杭州:16%,青岛:16%,其它:69%|
  * |  华东|商品_75|  366|  3|上海:17%,无锡:16%,其它:67%|
  * |  西北|商品_15|  116|  1|       西安:54%,银川:46%|
  * |  西北| 商品_2|  114|  2|       银川:54%,西安:46%|
  * |  西北|商品_22|  113|  3|       西安:55%,银川:45%|
  * |  华南|商品_23|  224|  1|厦门:29%,深圳:25%,其它:46%|
  * |  华南|商品_65|  222|  2|深圳:28%,厦门:27%,其它:45%|
  * |  华南|商品_50|  212|  3|福州:27%,深圳:26%,其它:47%|
  * |  华北|商品_42|  264|  1|保定:25%,郑州:25%,其它:50%|
  * |  华北|商品_99|  264|  2|北京:24%,郑州:23%,其它:52%|
  * |  华北|商品_19|  260|  3|郑州:23%,保定:20%,其它:56%|
  * |  东北|商品_41|  169|  1|哈尔滨:36%,大连:35%,其它...|
  * |  东北|商品_91|  165|  2|哈尔滨:36%,大连:33%,其它...|
  * |  东北|商品_58|  159|  3|沈阳:38%,大连:32%,其它:30%|
  * |  华中|商品_62|  117|  1|       武汉:51%,长沙:49%|
  * |  华中| 商品_4|  113|  2|       长沙:53%,武汉:47%|
  * |  华中|商品_57|  111|  3|       武汉:55%,长沙:45%|
  * |  西南| 商品_1|  176|  1|贵阳:36%,成都:36%,其它:28%|
  * |  西南|商品_44|  169|  2|贵阳:37%,成都:34%,其它:28%|
  * +----+-----+-----+---+--------------------+
  * only showing top 20 rows
  */

/**
  * 集合练习
  */
object AreaApp2 {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1,3,4,3,5,3,5,10)
    list.sortBy(x=> -x).foreach(println)

    val list2 = List(("a",30),("b",13),("c",4))
    list2.sortBy(-_._2).foreach(println)

    println("-------------")

    val map1: Map[String, Int] = Map("a"->14,"b"->12,"c"->23)
    val map2: Map[String, Int] = Map("a"->12,"c"->43,"g"->15)

    val m3: Map[String, Int] = map1.foldLeft(map2) {
      case (map, (k, v)) =>
        map + (k -> (map.getOrElse(k, 0) + v))
    }

    m3.foreach(println)

    //从左边开始计算，0为初始化值
    val i: Int = list.foldLeft(10)(_+_)
    println(i)
  }
}

package com.cjy.spark.T9_saveData

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD：读取mysql中数据
  */
object T5_MysqlRDD {

  //1. 初始化数据库配置
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/ccl"
  val userName = "root"
  val passWd = "root"

  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
//    val driver = "com.mysql.jdbc.Driver"
//    val url = "jdbc:mysql://localhost:3306/ccl"
//    val userName = "root"
//    val passWd = "root"

    //创建JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select name,age from `t_user` where `id`>=? and id<=?;",
      1,
      3,
      1,
      r => (r.getString(1), r.getInt(2))
    )
    //打印最后结果
    rdd.foreach(println)
    //--------------------------以上为数据读取

    val dataRDD: RDD[(String, Int)] = sc.parallelize(List(("赵六", 19), ("田七", 20), ("孙八", 24)))
    insert2(dataRDD)

    sc.stop()
  }

  /**
    * 插入函数方式一
    *     缺点：每插入一条数据获取一次数据库连接，spark进程与mysql进程之间交互过于频繁，不建议
    * @param dataRDD
    */
  def insert1(dataRDD: RDD[(String, Int)]): Unit ={
    //1.遍历处理每一条数据
    dataRDD.foreach {
      //2. 模式匹配
      case (name, age) => {
        Class.forName(driver)
        //3. 获取数据库连接
        val conn: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
        //4. 准备sql ，设置参数
        val sql = "insert into t_user(name,age) values(?,?)"
        val statement: PreparedStatement = conn.prepareStatement(sql)
        statement.setString(1,name)
        statement.setInt(2,age)
        //5. 执行
        statement.executeUpdate()
        //6. 关闭资源
        statement.close()
        conn.close()
      }
    }
  }

  /**
    * 每次处理一个分区数据，一个分区获取一次数据库连接
    *     注意：当有多个分区时，数据入库顺序会发生变化
    * @param dataRDD
    */
  def insert2(dataRDD: RDD[(String, Int)]): Unit ={
    //1. 每次处理一个分区
    dataRDD.foreachPartition(datas =>{
      //2. 一个分区获取一次数据库连接
      Class.forName(driver)
      val conn: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
      val sql = "insert into t_user(name,age) values(?,?)"

      //3. 处理每个分区内数据
      datas.foreach{
        //4. 模式配置数据
        case (name, age) =>{
          //设置参数，执行
          val statement: PreparedStatement = conn.prepareStatement(sql)
          statement.setString(1,name)
          statement.setInt(2,age)
          statement.executeUpdate()
          statement.close()
        }
      }
      //关闭连接
      conn.close()
    })
  }
}

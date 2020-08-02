package com.cjy.sparkstreaming.T2_DStreamCreate

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object T5_KafkaDStream3 {


  // 定义kafka 信息
  val groupId = "ccl"
  val topic = Set("ds-ccl")
  val param: Map[String, String] = Map[String, String](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    , "group.id" -> groupId)

  //创建kafka集群对象，用于获取、保存偏移量信息
  val cluster = new KafkaCluster(param)

  def main(args: Array[String]): Unit = {

    //配置、环境对象
    val conf: SparkConf = new SparkConf().setAppName("kafka ds - 01").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    //最后一个泛型表示返回值类型 handler.message()
    val dstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc, //环境对象
      param, //kafka信息
      readOffsets(), //消费偏移量信息
      (handler: MessageAndMetadata[String, String]) => handler.message() // 从kafka读到数据的value，handler可以获取其它信息
    )

    //计算
    dstream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print(1000)

    saveOffset(dstream)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取分区消费offset
    *
    * @return
    */
  def readOffsets() = {
    //1。定义最终返回的保存每个分区偏移信息 的 map
    var resultMap = Map[TopicAndPartition, Long]()
    //2。根据主题获取分区信息，Err 表示主题不存在
    val topicAndPartitionSetEither: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topic)

    topicAndPartitionSetEither match {
      case Right(topicAndPartitions) => {
        //3。根据消费者组id与主题所有分区信息获取消费者offset，map中是分区->消费偏移量
        val topicAndPartitionOffset: Either[Err, Map[TopicAndPartition, Long]] = cluster.getConsumerOffsets(groupId, topicAndPartitions)

        topicAndPartitionOffset match {
          //3。1 返回当前分区 offset
          case Right(offsetMap) => {
            resultMap ++= offsetMap
          }
          //3。2 获取不到消费偏移量，表示此分区第一次消费，则返回默认从零开始消费
          case _ => {
            topicAndPartitions.foreach(topicPartition => {
              resultMap += topicPartition -> 0L
            })
          }
        }
      }
      case _ => //表示主题不存在，什么都不做
    }
    resultMap
  }

  /**
    * 保存消费者偏移量
    * @param dstream
    */
  def saveOffset(dstream: InputDStream[String]) = {

    // 保存offset一定从kafka消费到的直接的那个Steram保存
    // 每个批次执行一次传递过去的函数
    dstream.foreachRDD(rdd => {
      //保存消费offset
      var map = Map[TopicAndPartition, Long]()

      // 如果这个rdd是直接来自于Kafka, 则可以强转成 HasOffsetRanges
      // 这类型就包含了, 这次消费的offsets的信息
      val hasOffset: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]

      //获取此次消费的所有分区的offset
      val ranges: Array[OffsetRange] = hasOffset.offsetRanges

      ranges.foreach(offset =>{
        val key: TopicAndPartition = offset.topicAndPartition()  //获取分区对象
        val value: Long = offset.untilOffset  //获取此次消费到哪里
        map += key -> value
      })
      //将此消费者组的消费对此分区数据的消费偏移量保存
      cluster.setConsumerOffsets(groupId,map)
    })
  }
}

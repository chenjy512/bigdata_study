package com.cjy.sparkstreaming.T2_DStreamCreate

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

object T2_MySourceSocket {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyReceiverDemo")
    val ssc = new StreamingContext(conf, Seconds(5))

    val sourceStream = ssc.receiverStream(new MySource("hadoop102", 9999))


    sourceStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}


/**
  * 自定义数据源：StorageLevel：存放等级，这里表示只在内存中
  * String：接收数据类型
  */
class MySource(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  override def onStart(): Unit = {

    new Thread("Socket Receiver"){
      override def run(): Unit = {
        receive()
      }
    }.start()
  }
  var socket: Socket = _
  var reader: BufferedReader = _

  def receive()={
    //创建 socket 对象，输入监控地址及端口
    socket = new Socket(host,port)
    //数据输入流对象
    //转为字符流，设置编码
    reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
    //每次读取的行数据
    var line:String =null;


    import scala.util.control.Breaks._
    breakable {
      while ((line = reader.readLine()) != null && socket.isConnected) {
//        if ("stopccl".equals(line)) {
//          break()
//        } else {
          //发送数据
          store(line)
//        }
      }
    }

    // 循环结束, 则关闭资源
    onStop()
//    if(!"stopccl".equals(line) ){
      // 重启任务
      restart("Trying to connect again")
//    }
  }

  override def onStop(): Unit = {
      if(reader != null){
        reader.close()
      }
    if(socket != null){
      socket.close()
    }
  }
}
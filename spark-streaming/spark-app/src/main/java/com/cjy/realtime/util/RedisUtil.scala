package com.cjy.realtime.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}


object RedisUtil {
  private val conf = new JedisPoolConfig

  conf.setMaxTotal(100)
  conf.setMaxIdle(10)
  conf.setMinIdle(10)
  conf.setBlockWhenExhausted(true) // 忙碌是否等待
  conf.setMaxWaitMillis(10000) // 最大等待时间
  conf.setTestOnBorrow(true)
  conf.setTestOnReturn(true)

  val pool = new JedisPool(conf, "hadoop102", 6379,30000,"redis")

  def getClient = pool.getResource

  def close(): Unit ={
    if(getClient != null ){
      getClient.close()
    }
    if (pool != null){
      pool.close()
    }
  }
  def main(args: Array[String]): Unit = {
    val str: String = getClient.get("k1")
    println(str)
//    getClient.close()
//    pool.close()
  }
}



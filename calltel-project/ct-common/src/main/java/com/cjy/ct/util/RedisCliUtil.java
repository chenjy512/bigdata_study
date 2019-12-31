package com.cjy.ct.util;

import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RedisCliUtil {

    private static Jedis jedis = null;

    public static Jedis getJedis(){
        if(jedis ==null ){
            synchronized (Object.class){
                if(jedis == null)
                 jedis = new Jedis("hadoop201", 6379);
                jedis.auth("redis");
            }
        }
        return  jedis;
    }


}

package com.cjy.cache;

import com.cjy.ct.util.JDBCUtil;
import com.cjy.ct.util.RedisCliUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BootStrap {

    public static void main(String[] args) {
        Jedis jedis = RedisCliUtil.getJedis();
        Connection conn = null;
        PreparedStatement pre = null;
        ResultSet resultSet = null;

        try {
            conn = JDBCUtil.getConnection();
            String queryUserSql = "select id, tel from ct_user";
            pre = conn.prepareStatement(queryUserSql);
            resultSet = pre.executeQuery();
            while (resultSet.next()) {
                int telId = resultSet.getInt(1);
                String num = resultSet.getString(2);
//                String key = "tel_" + num;
//                jedis.set(key, telId + "");
                jedis.hset("ct_tel",num,telId+"");
            }
            resultSet.close();

            String queryDateSql = "select id, year, month, day from ct_date";
            pre = conn.prepareStatement(queryDateSql);
            resultSet = pre.executeQuery();

            while (resultSet.next()) {
                int dateId = resultSet.getInt(1);
                String year = resultSet.getString(2);
                String month = resultSet.getString(3);
                String day = resultSet.getString(4);

                if (month.length() == 1) {
                    month = "0" + month;
                }
                if (day.length() == 1) {
                    day = "0" + day;
                }
                String key = year+month+day;
                jedis.hset("ct_date",key,dateId+"");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(resultSet != null){
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pre != null) {
                try {
                    pre.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if(jedis != null){
                jedis.close();
            }
        }
    }


}

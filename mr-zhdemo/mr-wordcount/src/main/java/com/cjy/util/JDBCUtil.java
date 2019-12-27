package com.cjy.util;


import com.cjy.format.bean.Dept;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {

    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/tsbds?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static Integer queryCount(){
        Connection conn = JDBCUtil.getConnection();
        Integer count = 0;
        String sql = "SELECT COUNT(*) FROM t_data";
        try {
            PreparedStatement pre = conn.prepareStatement(sql);
            ResultSet res = pre.executeQuery();
            res.next();
            count = res.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return count;
    }

    public static void main(String[] args) {
        //导入依赖测试链接
        System.out.println(getConnection());

         Connection conn = null;
         List<Dept> list  = new ArrayList<Dept>();
        conn = JDBCUtil.getConnection();

        String sql = "SELECT tjdw ,dwmc FROM t_dept;";

        try {
            PreparedStatement pre = conn.prepareStatement(sql);
            ResultSet resultSet = pre.executeQuery();
            while(resultSet.next()){
                String tjdw = resultSet.getString(1);
                String dwmc = resultSet.getString(2);
                Dept dept = new Dept(tjdw, dwmc);
                list.add(dept);
            }
            resultSet.close();
            pre.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
    }
}

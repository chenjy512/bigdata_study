package com.cjy.mr.mysqlformat;


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

    //数据切片准备
    public static Integer queryCount(){
        Connection conn = JDBCUtil.getConnection();
        Integer count = 0;
        String sql = "SELECT COUNT(*) FROM t_order";
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
         List<Order> list  = new ArrayList<Order>();
        conn = JDBCUtil.getConnection();

        String sql = "SELECT id,user_code,product_code,value FROM t_order;";

        try {
            PreparedStatement pre = conn.prepareStatement(sql);
            ResultSet resultSet = pre.executeQuery();
            while(resultSet.next()){
                String id = resultSet.getString(1);
                String usercode = resultSet.getString(2);
                String productCode = resultSet.getString(3);
                String value = resultSet.getString(4);
                Order order = new Order(id, usercode,productCode,Integer.parseInt(value));
                list.add(order);
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

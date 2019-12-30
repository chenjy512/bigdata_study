package com.cjy.hbase.Util;




import java.sql.*;
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
        String sql = "SELECT COUNT(*) FROM fruit";
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
    }
}

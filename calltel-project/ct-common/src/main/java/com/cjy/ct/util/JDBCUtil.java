package com.cjy.ct.util;

import com.cjy.ct.constant.ConstantUrl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCUtil {

        private static final String DRIVER= ConstantUrl.getProperty("jdbc.driver");
        private static final String URL = ConstantUrl.getProperty("jdbc.url");
        private static final String USERNAME=ConstantUrl.getProperty("jdbc.username");
        private static final String PASSWORD=ConstantUrl.getProperty("jdbc.password");

        public static Connection getConnection(){
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

    public static void main(String[] args) {
            //导入依赖测试链接
        System.out.println(getConnection());
    }
}

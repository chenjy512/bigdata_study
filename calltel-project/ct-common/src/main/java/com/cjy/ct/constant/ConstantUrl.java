package com.cjy.ct.constant;

import java.io.IOException;
import java.util.Properties;

public class ConstantUrl {

    private volatile Properties prop = null;


    private static JdbcUrl urlVo = new JdbcUrl();

    public static String getProperty(String key){

        return urlVo.getProperty(key);
    }


    private static class JdbcUrl{
        private volatile Properties prop = null;
        public JdbcUrl(){
            prop = new Properties();
            try {
                prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("jdbc.properties"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        public String getProperty(String key){
            return prop.getProperty(key);
        }
    }

    public static void main(String[] args) {
        System.out.println(ConstantUrl.getProperty("jdbc.url"));
    }
}



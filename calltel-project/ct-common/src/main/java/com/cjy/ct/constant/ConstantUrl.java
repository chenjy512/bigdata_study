package com.cjy.ct.constant;

import java.io.IOException;
import java.util.Properties;

public class ConstantUrl {

    private volatile Properties prop = null;

    public static String getProperty(String key){

        return prop.getProperty(key);
    }

    private class JdbcUrl{
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
}



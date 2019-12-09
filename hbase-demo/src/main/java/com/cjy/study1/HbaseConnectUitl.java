package com.cjy.study1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;

import java.io.IOException;

/**
 * 获取连接
 */
public class HbaseConnectUitl {

    private HbaseConnectUitl() {
    }

    private static Connection connection = null;
    private static Admin admin = null;
    private static Table table = null;
    /**
     * 获取连接
     * @return
     * @throws IOException
     */
    public static Connection getConnect() throws IOException {
        if (connection == null) {
            synchronized (Object.class) {
                if (connection == null) {
                    Configuration configuration = HBaseConfiguration.create();
                    //设置zk集群位置
//                    configuration.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
                    //添加配置文件的话就不用再显示指定了 hbase-default.xml
                    configuration.set("hbase.zookeeper.quorum", "hadoop202,hadoop203,hadoop204");
                    connection = ConnectionFactory.createConnection(configuration);
                }
            }
        }
        return connection;
    }

    /**
     * 获取Admin
     */
    public static Admin getAdmin(){
        if(admin ==null){
            synchronized (Object.class){
                if(admin ==null){
                    try {
                        Connection connect = getConnect();
                        admin = connection.getAdmin();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return admin;
    }
    /**
     * 获取Admin
     */
    public static Table getTable(String tableName) throws IllegalAccessException {
        if (StringUtils.isEmpty(tableName))
            throw new IllegalAccessException("tableName is empty");
        if(table ==null){
            synchronized (Object.class){
                if(table ==null){
                    try {
                        Connection connect = getConnect();
                        table = connection.getTable(TableName.valueOf(tableName));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return table;
    }
    /**
     * 资源关闭
     */
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

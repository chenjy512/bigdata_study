package com.cjy.util;

import com.cjy.constans.Constans;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
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
                    configuration.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
//                    configuration.set("hbase.zookeeper.quorum", "hadoop202,hadoop203,hadoop204");
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
/*    public static Table getTable(String tableName) throws IllegalAccessException {
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
    }*/
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

    /**
     * 判断命名空间是否存在
     * @param ns 命名空间名称
     * @return
     * @throws IllegalAccessException
     */
    public static boolean isNameSpaceExist(String ns) throws IllegalAccessException {
        //1.参数判断
        if (StringUtils.isEmpty(ns))
            throw new IllegalAccessException("ns is not exist");
        //2. 获取admin
        Admin admin = HbaseConnectUitl.getAdmin();
        boolean exist = true;
        try {
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(ns);
//            String name = namespaceDescriptor.getName();
//            System.out.println(name);
        }catch (NamespaceNotFoundException e){
//            System.out.println("NamespaceNotFoundException");
            exist = false;
        }catch (IOException e) {
            e.printStackTrace();
        }
        return exist ;
    }
    /**
     * 创建命名空间
     * @param ns 命名空间名称
     * @throws IllegalAccessException
     * @throws IOException
     */
    public static void createNameSpace(String ns) throws IllegalAccessException, IOException {
        //1.参数判断
        if (StringUtils.isEmpty(ns))
            throw new IllegalAccessException("ns is not null");
        if (isNameSpaceExist(ns))
            throw new IllegalAccessException(ns+" NameSpace is  exist");

        //2. 获取admin
        Admin admin = HbaseConnectUitl.getAdmin();
        //3. 创建表空间
        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.out.println(ns+" 命名空间存在");
        }
    }

    /**
     * 删除命名空间
     * @param ns 命名空间名称
     * @throws IllegalAccessException
     * @throws IOException
     */
    public static void dropNameSpace(String ns) throws IllegalAccessException, IOException {
        //1.参数判断
        if (StringUtils.isEmpty(ns))
            throw new IllegalAccessException("ns is not null");
        if (!isNameSpaceExist(ns))
            throw new IllegalAccessException(ns+" NameSpace is not exist");

        //2. 获取admin
        Admin admin = HbaseConnectUitl.getAdmin();
//        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();
        admin.deleteNamespace(ns);
    }

    public static void main(String[] args) throws IOException, IllegalAccessException {
        //测试
//        Connection connect = getConnect();
//        System.out.println(connect);

//        createNameSpace(Constans.NAMA_SPACE);
//        close();

    }
}

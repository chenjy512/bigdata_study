package com.cjy.ct.bean;

import com.cjy.ct.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * 1.导入hbase依赖
 * 2.导入hbase-site.xml，这样就不用指定配置了，因为hbase配置类会默认读取类路径下此名称的配置文件
 *
 */
public abstract class BaseDao {

        //将连接对象绑定到当前线程中，避免多线程重复创建连接
    ThreadLocal<Connection> connHolder = new ThreadLocal<>();
    ThreadLocal<Admin> adminHolder = new ThreadLocal<>();


    /**
     * 获取连接
     * @return
     * @throws IOException
     */
    public Connection getConn() throws IOException {
        Connection connection = connHolder.get();
        if(connection == null){
            synchronized (Object.class){
                if(connection == null){
                    //create：默认去查找类路径下    conf.addResource("hbase-default.xml"); conf.addResource("hbase-site.xml");
                    Configuration config = HBaseConfiguration.create();
                    connection = ConnectionFactory.createConnection(config);
                    connHolder.set(connection);
                }
            }
        }
        return connection;
    }

    /**
     * 获取表元数据操作对象
     * @return
     * @throws IOException
     */
    public Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if(admin == null ){
            synchronized (Object.class){
                if(admin == null){
                    admin = getConn().getAdmin();
                    adminHolder.set(admin);
                }
            }
        }
        return admin;
    }

    /**
     * 获取资源
     */
    protected void start() throws IOException {
        getConn();
        getAdmin();
    }

    /**
     * 资源释放
     * @throws IOException
     */
    protected void end() throws IOException {
        Admin admin = getAdmin();
        if(admin != null){
            admin.close();
            adminHolder.remove();
        }
        Connection conn = getConn();
        if(conn != null){
            conn.close();
            connHolder.remove();
        }
    }

    /**
     * 创建命名空间，存在则不操作
     * @param namespace
     */
    protected void createNamespaceNX(String namespace) throws IOException {
        //1. 获取连接
        Admin admin = getAdmin();
        try {
            //2. 获取命名空间描述器
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(namespace);
        }catch (NamespaceNotFoundException e){
            //3.不存在则新建命名空间
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 创建表，之前存在则删除
     * @param tableName
     * @param families
     * @throws IOException
     */
    protected void createTalbeXX(String tableName,Integer regionCount,String...families) throws IOException {
        Admin admin = getAdmin();
        //判断
        if(admin.tableExists(TableName.valueOf(tableName))){
            //表存在，删除表
            deleteTable(tableName);
        }
        //未设置列族，指定默认列族
        if(families == null || families.length == 0){
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }
        //准备描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : families) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //创建表
        if(regionCount == null || regionCount ==0){
            admin.createTable(hTableDescriptor);
        }else{

        }

    }

    /**
     * 生成分区主键
     * @param count 分区个数
     * @return
     */
    private byte[][] genSplitKeys(Integer count){
        //分区个数-1 个主键
        int keyCount = count - 1;
        byte[][] bs = new byte[keyCount][];
        for (int i = 0;i < keyCount;i++){

        }
        return bs;
    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    protected void deleteTable(String tableName) throws IOException {
        Admin admin = getAdmin(); //1. 获取对象
        TableName tableName1 = TableName.valueOf(tableName);
        admin.disableTable(tableName1);//2. 禁用表
        admin.deleteTable(tableName1); //3. 删除表
    }


    /**
     * 插入单条数据
     * @param tableName
     * @param put
     * @throws IOException
     */
    protected void putData(String tableName,Put put) throws IOException {
        Connection conn = getConn();
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = conn.getTable(tableName1);
        table.put(put);
        table.close();
    }

    /**
     * 插入多条数据
     * @param tableName
     * @param puts
     * @throws IOException
     */
    protected void putData(String tableName,List<Put> puts) throws IOException {
        Table table = getConn().getTable(TableName.valueOf(tableName));
        table.put(puts);
        table.close();
    }

    public static void main(String[] args) {

    }
}

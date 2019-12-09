package com.cjy.ct.bean;

import com.cjy.ct.api.Column;
import com.cjy.ct.api.RowKey;
import com.cjy.ct.api.TableRef;
import com.cjy.ct.constant.Names;
import com.cjy.ct.constant.ValConstant;
import com.cjy.ct.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
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
     * @param tableName 表名
     * @param families 列族
     * @param regionCount 分区数
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
            byte[][] region = genSplitKeys(regionCount);
            admin.createTable(hTableDescriptor,region);
        }

    }

    protected void createTalbeXX(String tableName,String...families) throws IOException {

            createTalbeXX(tableName,null,families);
    }
    /**
     * 生成分区号
     * @param count 分区个数
     * @return
     */
    private  byte[][] genSplitKeys(Integer count){
        //假设6个分区
        /**
         * -,0|
         * 0|,1|
         * 1|,2|
         * 2|,3|
         * 3|,4|
         * 4|,+
         */
        int keyCount = count - 1;
        byte[][] bs = new byte[keyCount][];
        for (int i = 0;i < keyCount;i++){
            String part = i+"|";
//            System.out.println(part);
            bs[i] = Bytes.toBytes(part);
        }
        return bs;
    }

    /**
     * 计算分区号
     * @param tel 电话号码
     * @param date 日期
     * @return 分区号  0,1,2,3,4,5
     */
    protected  Integer getRegionNum(String tel,String date){
            tel = tel.substring(tel.length()-5);
            date = date.substring(0,6); //截取到月份 20180303122343 -> 201803
            int th = tel.hashCode();
            int dh = date.hashCode();
            int num = Math.abs(th^dh);
            int regionNum = num % ValConstant.REGION_COUNT;
            return regionNum;
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
     * put对象，使用注解方式
     * @param obj
     */
    protected void putData(Object obj) throws IllegalAccessException, IOException {
        //反射使用，获取类实例
        Class<?> cls = obj.getClass();
        //获取表注解
        TableRef tableRef = cls.getAnnotation(TableRef.class);
        //获取注解值
        String tableName = tableRef.value();
        //获取所有属性对象
        Field[] fields = cls.getDeclaredFields();
        String rowKeyValue = "";
        for (Field field : fields) {
            RowKey rowKeyAnno = field.getAnnotation(RowKey.class);
            if(rowKeyAnno != null){
                //属性由于是私有的所以需要权限
                    field.setAccessible(true);
                    //获取属性值
                rowKeyValue = (String)field.get(obj);
                break;
                }
        }

        //获取连接
        Connection conn = getConn();
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = conn.getTable(tableName1);
        Put put = new Put(Bytes.toBytes(rowKeyValue));

        for (Field field : fields) {
            //判断此数据是否为column注解标注
            Column column = field.getAnnotation(Column.class);
            if(column != null){
                //是，则取其列族
                String family = column.family();
                //取列名，判断为空则使用属性名
                String cn = column.column();
                if(StringUtil.isEmpty(cn)){
                    //获取属性名
                    cn = field.getName();
                }
                //获取值
                field.setAccessible(true);
                String value = (String) field.get(obj);
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(cn),Bytes.toBytes(value));
            }
        }

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
//        byte[][] bytes = genSplitKeys(5);

        /*System.out.println(getRegionNum("18411925860","20180415213918"));
        System.out.println(getRegionNum("18944239644","20181111060747"));
        System.out.println(getRegionNum("15133295266","20180331021611"));
        System.out.println(getRegionNum("19920868202","20180806042914"));*/
    }
}

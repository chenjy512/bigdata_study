package com.cjy.study1;

import com.cjy.study1.beans.RowData;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * 常用API操作测试
 */
public class HbaseOperatorCURD {
    //-----------------------------------------------------以下是 DDL 操作
    /**
     * 1. 判断表是否存在
     * @param tableName 表名称
     * @return
     */
    public static boolean isTableExist(String tableName) throws IOException, IllegalAccessException {
        if (StringUtils.isEmpty(tableName))
            throw new IllegalAccessException("tableName is not null");

        Admin admin = HbaseConnectUitl.getAdmin();
        //注意TableName是个类
        boolean res = admin.tableExists(TableName.valueOf(tableName));
        return res;
    }

    /**
     * 2. 创建表
     * @param tableName 表名称
     * @param clumnFamilys 列族，多个列族
     */
    public static void createTable(String tableName, String... clumnFamilys) throws IllegalAccessException, IOException {
        //1.参数判断
        if (StringUtils.isEmpty(tableName))
            throw new IllegalAccessException("tableName is not null");
        if (clumnFamilys == null || clumnFamilys.length == 0)
            throw new IllegalAccessException("clumnFamilys is not null");
        if(isTableExist(tableName))
            throw new IllegalAccessException(tableName + " table is exist");
        //2. 获取admin
        Admin admin = HbaseConnectUitl.getAdmin();
        //3. 创建表描述
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //4. 封装列族信息
        for (String clumnFamily : clumnFamilys) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(clumnFamily);
            //添加列族后返回本身对象，因为实际存放列族信息的位置是---> this.families.put(family.getName(), family);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    /**
     * 3. 删除表
     * @param tableName 表名称
     * @throws IllegalAccessException
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IllegalAccessException, IOException {
        //1.参数判断
        if (StringUtils.isEmpty(tableName))
            throw new IllegalAccessException("tableName is not null");
        if(!isTableExist(tableName))
            throw new IllegalAccessException(tableName + " table is not exist");
        //2. 获取admin
        Admin admin = HbaseConnectUitl.getAdmin();
        //3. 删除表
        //3.1 表下线
        admin.disableTable(TableName.valueOf(tableName));
        //3.2 表删除
        admin.deleteTable(TableName.valueOf(tableName));
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

    //-----------------------------------------------------以下是 DML 操作

    /**
     * 1. 向表中添加单条数据
     * @param tableName 表名
     * @param rowKey key
     * @param cf 列族
     * @param cn 列名
     * @param value 列值
     * @throws IOException
     */
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        //1. 参数判断省略

        //2. 获取表操作对象
        Table table = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(tableName));
        //3. 封装参数
        Put put = new Put(Bytes.toBytes(rowKey)); //需要rowkey
        //添加列族、列名、值
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //4. 添加操作
        table.put(put);
        //5. 关闭资源
        table.close();
    }

    //1. 向表中添加单行多列数据数据
    public static void putRowDatas(String tableName, String rowKey, String cf, String cn[], String value[]) throws IOException {
        //1. 参数判断省略

        //2. 获取表操作对象
        Table table = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(tableName));
        //3. 封装参数
        List<Put> puts = new ArrayList<>(cf.length());
        //多列数据封装
        for (int i = 0;i < cn.length;i++){
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn[i]),Bytes.toBytes(value[i]));
            puts.add(put);
        }
        //4. 添加操作
        table.put(puts);
        //5. 关闭资源
        table.close();
    }

    /**
     * 添加多行数
     * @param tableName 表名
     * @param datas 数据集
     * @throws IOException
     */
    public static void bathPutRowDatas(String tableName, List<RowData> datas) throws IOException {
        for (RowData data : datas) {
            putRowDatas(tableName,data.getRowKey(),data.getCf(),data.getCns(),data.getValues());
        }
    }

    /**
     * 查询某个列值
     * @param tableName 表名
     * @param rowKey 主键
     * @param cf 列族
     * @param cn 列名
     * @throws IOException
     */
    public static Map<String,Object> getRowData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Map<String,Object> map = new HashMap<String,Object>(1);
        Table table = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(tableName));
       //设置查询条件
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        Result result = table.get(get);
        //每行单元格数据数组
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" +
                    Bytes.toString(CellUtil.cloneValue(cell)));
            map.put(cn,Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
        return map;
    }

    public static void scanRowDatas(String tableName) throws IOException {
        Table table = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result result = iterator.next();
            for (Cell cell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) + "，CF:" +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    public static void main(String[] args) throws IOException, IllegalAccessException {
//        System.out.println(isTableExist("stu2"));
//        createTable("nsc:stu2","info");

//        dropTable("nsc:stu2");
//        System.out.println(isTableExist("stu2"));
//        createNameSpace("nsc");
//        dropNameSpace("nsc");
//        System.out.println(isNameSpaceExist("nsc"));
//        putData("stu","1001","info","sex","male");
//        putRowDatas("stu","1002","info",new String[]{"name","sex","age"},
//                new String[]{"lisi","famale","21"});
//        List<RowData> datas = new ArrayList<>();
//        datas.add(new RowData("1003","info",new String[]{"name","sex","age"},new String[]{"wangwu","male","12"}));
//        datas.add(new RowData("1004","info",new String[]{"name","sex"},new String[]{"zhaoliu","famale"}));
//        datas.add(new RowData("1005","info",new String[]{"name","sex","age","email"},new String[]{"tianqi","male","15","tianqi@126.com"}));
//
//        bathPutRowDatas("stu",datas);
//        getRowData("stu","1001","info","sex");
        scanRowDatas("stu");
        HbaseConnectUitl.close();
    }

}

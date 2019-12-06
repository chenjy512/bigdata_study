package com.cjy.dao;

import com.cjy.constans.Constans;
import com.cjy.util.HbaseConnectUitl;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * dao层
 */
public class WeiboDao {
    /**
     * 发布微博
     *
     * @param uid     发布人id
     * @param content 发布内容
     */
    public static void createWeibo(String uid, String content) throws IllegalAccessException, IOException {
        //1.获取微博表
        Table table = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.CONTENT_TABLE));
        //1.2. 封装put数据
        long time = new Date().getTime();
        String contenRowKey = uid + "_" + time;
        Put put = new Put(Bytes.toBytes(contenRowKey));
        put.addColumn(Bytes.toBytes(Constans.CONTENT_TABLE_CF), Bytes.toBytes(Constans.CONTENT_TABLE_CN), Bytes.toBytes(content));
        table.put(put);
        table.close();
        //2. 获取粉丝数据,获取关系表对象
        Table relTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.RELATION_TABLE));
        //2.2 设置查询数据条件
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constans.RELATION_TABLE_CF2));
        Result result = relTable.get(get);
        //2.3 取出粉丝
        Cell[] cells = result.rawCells();

        List<Put> inboxList = new ArrayList();
        for (Cell cell : cells) {
            //2.4 设置收件箱数据
            Put fansPut = new Put(CellUtil.cloneQualifier(cell));//获取粉丝id
            //封装数据
            fansPut.addColumn(Bytes.toBytes(Constans.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(contenRowKey));
            //收件箱数据格式多版本
            //zhangsan  info:lisi   contenkey1  2019-12-11
            //zhangsan  info:lisi   contenkey2  2019-12-12
            //zhangsan  info:wangwu   contenkey1    2019-12-12
            inboxList.add(fansPut);
        }
        relTable.close();
        if (inboxList.size() < 1) {
            return;
        }
        //3. 更新粉丝收件箱
        Table inboxTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.INBOX_TABLE));
        inboxTable.put(inboxList);

        inboxTable.close();
    }

    /**
     * 关注用户
     *
     * @param uid
     * @param gzids
     */
    public static void guanzuUser(String uid, String... gzids) throws IOException {
        if (gzids.length < 1) {
            System.out.println("关注用户数据为空");
            return;
        }

        //1.增加attends明星列
        //1.1 获取用户关系表
        Table relTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.RELATION_TABLE));
        //1.2存放用户明星列数据
        List<Put> relPuts = new ArrayList<Put>();
        //1.3 遍历处理新关注明星
        for (String gzid : gzids) {
            Put put = new Put(Bytes.toBytes(uid));
            //1.4 封装数据； 数据格式如：put 'weibo:relation','zhangsan','attends:gzid','1'
            put.addColumn(Bytes.toBytes(Constans.RELATION_TABLE_CF1), Bytes.toBytes(gzid), Bytes.toBytes("1"));
            relPuts.add(put);
        }
//        relTable.put(relPuts); //处理关注
//        relTable.close();
        //2.修改被关注用户的fans列
        //2.1 每个被关注的明星增加粉丝 uid
        for (String gzid : gzids) {
            Put gzPut = new Put(Bytes.toBytes(gzid));
            gzPut.addColumn(Bytes.toBytes(Constans.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes("1"));
//            relTable.put(gzPut); //处理被关注
            relPuts.add(gzPut);
        }
        //关注与被关注可一起处理；
        relTable.put(relPuts);
        relPuts.clear();//便于多次使用
        relTable.close();

        //3.增加收件箱，获取新关注用户的最近三条微博
        //3.1 获取信息表、收件箱对象
//        Table conTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.CONTENT_TABLE));
        Table inTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.INBOX_TABLE));
        //3.2 遍历取出关注者的最前面三条微博rowkey

        for (String gzid : gzids) {
            List<String> contenKys = queryContentByUid(gzid, 3);
            for (String contenKy : contenKys) {
                Put inPut = new Put(Bytes.toBytes(uid));
                inPut.addColumn(Bytes.toBytes(Constans.INBOX_TABLE_CF), Bytes.toBytes(gzid), Bytes.toBytes(contenKy));
//                relPuts.add(inPut);
                inTable.put(inPut); //同一条数据多个版本添加只能一条一条添加，覆盖才能生效
            }
        }
//        inTable.put(relPuts);  //注意对一条数据多个版本同时添加是不能使用list，否则只有一条生效
//        conTable.close();
        inTable.close();
    }

    /**
     * 取出某个用户的几次微博
     *
     * @param uid 用户
     * @param cs  次数
     * @return
     * @throws IOException
     */
    public static List<String> queryContentByUid(String uid, int cs) throws IOException {
        Table conTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.CONTENT_TABLE));
        Scan scan = new Scan();
        //获取某个用户所有微博
        scan.setStartRow(Bytes.toBytes(uid + "_"));
        scan.setStopRow(Bytes.toBytes(uid + "|"));
        ResultScanner scanner = conTable.getScanner(scan);
        List<String> list = new ArrayList<String>(3);
        //设置条件取其前三条
        cs = cs < 3 ? 3 : cs;
        int count = 0;
        for (Result result : scanner) {

            if (count++ >= cs) {
                continue;
            }
            Cell[] cells = result.rawCells();
            //由于信息表只有一列数据不用遍历
//            System.out.println(Bytes.toString(cells[0].getRowArray()));
//            for (Cell cell : cells) {
            System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cells[0])) + "\t " +
                    "cf:" + Bytes.toString(CellUtil.cloneFamily(cells[0])) + "\t " +
                    "cn:" + Bytes.toString(CellUtil.cloneQualifier(cells[0])) + "\t " +
                    "value:" + Bytes.toString(CellUtil.cloneValue(cells[0])));
            list.add(Bytes.toString(CellUtil.cloneRow(cells[0])));
//            }
        }
        return list;
    }

    /**
     * 取消关注
     *
     * @param uid
     * @param gzids
     * @throws IOException
     */
    public static void quxgzUser(String uid, String... gzids) throws IOException {
        if (gzids.length < 1) {
            System.out.println("取消关注用户数据为空");
            return;
        }
        //1. 处理用户关系表--取消明星，取消被关注者的粉丝
        //1.1 获取关系表对象
        Table relTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.RELATION_TABLE));
        //1.2 取消用户关注明星
        List<Delete> relPuts = new ArrayList<Delete>();
        for (String gzid : gzids) {
            Delete delPut = new Delete(Bytes.toBytes(uid));
            delPut.addColumns(Bytes.toBytes(Constans.RELATION_TABLE_CF1), Bytes.toBytes(gzid));
            relPuts.add(delPut);
        }
        //1.3 移除被关注明星的粉丝
        for (String gzid : gzids) {
            Delete delPut = new Delete(Bytes.toBytes(gzid));
            delPut.addColumns(Bytes.toBytes(Constans.RELATION_TABLE_CF2), Bytes.toBytes(uid));
            relPuts.add(delPut);
        }
        relTable.delete(relPuts);
        relTable.close();
        //2. 收件箱历史是否删除待定
    }

    /**
     * 获取某个用户收件箱
     *
     * @param uid
     */
    public static void queryWeiboInbox(String uid) throws IOException {
        //1.获取我关注的所有明星发布微博rowkey
        //1.2 获取信息表对象，设置查询条件
        Table inTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.INBOX_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions(Constans.INBOX_TABLE_VERSIONS);
        get.addFamily(Bytes.toBytes(Constans.INBOX_TABLE_CF));
        //获取结果遍历处理，得到key
        Result result = inTable.get(get);
        Cell[] cells = result.rawCells();
        List<String> rowKeyList = new ArrayList<>();
        for (Cell cell : cells) {
            //取出列值的信息
            System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) + "，CF:" +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" +
                    Bytes.toString(CellUtil.cloneValue(cell)) +
                    "，time:" + cell.getTimestamp());
            rowKeyList.add(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        inTable.close();
        System.out.println("---------------------微博内容信息--------------------");
        //2.获取微博内容
        //2.2 获取微博内容表对象，根据key获取内容
        Table conTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.CONTENT_TABLE));
        for (String s : rowKeyList) {
            Get conGet = new Get(Bytes.toBytes(s));
            conGet.addColumn(Bytes.toBytes(Constans.CONTENT_TABLE_CF), Bytes.toBytes(Constans.CONTENT_TABLE_CN));
            Result conResult = conTable.get(conGet);
            Cell[] conCells = conResult.rawCells();
            for (Cell cell : conCells) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) + "，CF:" +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" +
                        Bytes.toString(CellUtil.cloneValue(cell)) +
                        "，time:" + cell.getTimestamp());
            }
            conTable.close();
        }
    }

    /**
     * 取出某个用户关注的所有明星
     * @param uid
     */
    public static void attendsList(String uid) throws IOException {
        Table relTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.RELATION_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constans.RELATION_TABLE_CF1));
        Result result = relTable.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) + "，CF:" +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" +
                    Bytes.toString(CellUtil.cloneValue(cell)) +
                    "，time:" + cell.getTimestamp());
        }
    }

    /**
     * 取出某个用户的所有粉丝列表
     * @param uid
     * @throws IOException
     */
    public static void fansList(String uid) throws IOException {
        Table relTable = HbaseConnectUitl.getConnect().getTable(TableName.valueOf(Constans.RELATION_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constans.RELATION_TABLE_CF2));
        Result result = relTable.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) + "，CF:" +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + "，CN:" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "，Value:" +
                    Bytes.toString(CellUtil.cloneValue(cell)) +
                    "，time:" + cell.getTimestamp());
        }
    }

    public static void main(String[] args) throws IOException, IllegalAccessException {
        //创建表空间
//        HbaseConnectUitl.createNameSpace(Constans.NAMA_SPACE);
        //测试微博发布
//        createWeibo("zhangsan","我是张三，我发微博了~~~");
        //测试获取某个用户前三条微博rowkey
//        queryContentByUid("zhangsan",2);
        //测试关注用户
//        guanzuUser("zhaoliu","lisi");
        //测试取关用户
//        quxgzUser("lisi","zhangsan");
        //查询某个用户收件箱
//        queryWeiboInbox("wangwu");
        //查询某个用户关注列表
//        attendsList("wangwu");
        //查询某个用户粉丝列表
        System.out.println();
//        fansList("wangwu");
        HbaseConnectUitl.close();
    }
}

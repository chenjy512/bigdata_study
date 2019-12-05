package com.cjy.dao;

import com.cjy.constans.Constans;
import com.cjy.util.HbaseConnectUitl;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
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
     * @param uid  发布人id
     * @param content 发布内容
     */
    public static void createWeibo(String uid,String content) throws IllegalAccessException, IOException {
        //1.获取微博表
        Table table = HbaseConnectUitl.getTable(Constans.CONTENT_TABLE);
        //1.2. 封装put数据
        long time = new Date().getTime();
        String contenRowKey = uid+"_"+time;
        Put put = new Put(Bytes.toBytes(contenRowKey));
        put.addColumn(Bytes.toBytes(Constans.CONTENT_TABLE_CF),Bytes.toBytes(Constans.CONTENT_TABLE_CN),Bytes.toBytes(content));
        table.put(put);
        table.close();
        //2. 获取粉丝数据,获取关系表对象
        Table relTable = HbaseConnectUitl.getTable(Constans.RELATION_TABLE);
        //2.2 设置查询数据条件
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constans.RELATION_TABLE_CF2));
        Result result = relTable.get(get);
        //2.3 取出粉丝
        Cell[] cells = result.rawCells();
        List<Put> inboxList = new ArrayList(cells.length);
        for (Cell cell : cells) {
            //2.4 设置收件箱数据
            Put fansPut = new Put(CellUtil.cloneQualifier(cell));//获取粉丝id
            //封装数据
            fansPut.addColumn(Bytes.toBytes(Constans.INBOX_TABLE_CF),Bytes.toBytes(uid),Bytes.toBytes(contenRowKey));
            //收件箱数据格式多版本
            //zhangsan  info:lisi   contenkey1  2019-12-11
            //zhangsan  info:lisi   contenkey2  2019-12-12
            //zhangsan  info:wangwu   contenkey1    2019-12-12
            inboxList.add(fansPut);
        }
        relTable.close();
        if(inboxList.size() < 1){
            return ;
        }
        //3. 更新粉丝收件箱
        Table inboxTable = HbaseConnectUitl.getTable(Constans.INBOX_TABLE);
        inboxTable.put(inboxList);
        inboxTable.close();
    }

//    public void

    public static void main(String[] args) throws IOException, IllegalAccessException {
        createWeibo("zhangsan","我叫张三，我发了条数据，粉丝有lisi、wangwu");
    }
}

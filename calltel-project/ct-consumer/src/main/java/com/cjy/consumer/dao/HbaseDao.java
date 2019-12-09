package com.cjy.consumer.dao;

import com.cjy.consumer.bean.Calllog;
import com.cjy.ct.bean.BaseDao;
import com.cjy.ct.constant.Names;
import com.cjy.ct.constant.ValConstant;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDao extends BaseDao {

    /**
     * 初始化建表空间与表
     * @throws IOException
     */
    public void init() throws IOException {
        start();
        createNamespaceNX(Names.NAMESTPACE.getValue());
        createTalbeXX(Names.TABLE.getValue(), ValConstant.REGION_COUNT,Names.CF_CALLER.getValue());
        end();
        System.out.println("初始化成功...");
    }

    /**
     * 数据插入
     * @param value
     */
    public void insertData(String value) throws IOException {
        // 1. 获取通话日志数据
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        //1.获取分区号
        Integer regionNum = getRegionNum(call1, calltime);

        //2.组装key
        String rowKey = regionNum+"_"+call1+"_"+calltime+"_"+call2+"_"+duration;
        //3.封装数据，插入
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        putData(Names.TABLE.getValue(),put);
    }

    public void insertData(Calllog log) throws IOException, IllegalAccessException {
        log.setRowKey(getRegionNum(log.getCall1(), log.getCalltime()) + "_" + log.getCall1() + "_" + log.getCalltime() + "_" + log.getCall2() + "_" + log.getDuration());
        putData(log);
    }
}

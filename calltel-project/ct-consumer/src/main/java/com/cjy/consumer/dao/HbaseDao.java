package com.cjy.consumer.dao;

import com.cjy.consumer.bean.Calllog;
import com.cjy.consumer.coprocessor.InserCallTelCoprocessor;
import com.cjy.ct.bean.BaseDao;
import com.cjy.ct.constant.Names;
import com.cjy.ct.constant.ValConstant;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 消费者数据插入hbase
 */
public class HbaseDao extends BaseDao {

    /**
     * 初始化建表空间与表
     * @throws IOException
     */
    public void init() throws IOException {
        start();
        createNamespaceNX(Names.NAMESTPACE.getValue());
        //设置多个列族
//        createTalbeXX(Names.TABLE.getValue(),"com.cjy.consumer.coprocessor.InserCallTelCoprocessor", ValConstant.REGION_COUNT,Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());
        createTalbeXX(Names.TABLE.getValue(), InserCallTelCoprocessor.class.getName(), ValConstant.REGION_COUNT,Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());
        end();
        System.out.println("初始化成功...");
    }

    //通过类的方式也可以，但是需要导入协处理器工程依赖
    protected void createTalbeXX(String tableName, Class cls, Integer regionCount, String... families) throws IOException {
        super.createTalbeXX(tableName, cls.getName(), regionCount, families);
    }

    /**
     * 数据插入
     * @param value 数据字符串
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
        String rowKey = regionNum+"_"+call1+"_"+calltime+"_"+call2+"_"+duration+"_"+ValConstant.NUM_ZJ;
        //3.封装数据，插入
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("flag"),Bytes.toBytes(ValConstant.NUM_ZJ));

        putData(Names.TABLE.getValue(),put);
    }

    /**0：主叫
     * 1：被叫
     * 数据插入--主叫、被叫用户数据
     * @param value 数据字符串
     */
    public void insertDataZB(String value) throws IOException {
        // 1. 获取通话日志数据，解析
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        List<Put> putList = new ArrayList<Put>(2); //存放主叫，被叫数据
        //------------------设置主叫数据
        //1.获取分区号
        Integer regionNum = getRegionNum(call1, calltime);
        //2.组装key
        String rowKey = regionNum+"_"+call1+"_"+calltime+"_"+call2+"_"+duration+"_"+ValConstant.NUM_ZJ;
        //3.封装数据，插入
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(Bytes.toBytes(Names.CF_CALLER.getValue()),Bytes.toBytes("flag"),Bytes.toBytes(ValConstant.NUM_ZJ));

        putList.add(put);
        //------------------设置被叫数据
        Integer regionNumB = getRegionNum(call2, calltime);
        String bjRowKey = regionNumB + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_" + ValConstant.NUM_BJ;

        Put putB = new Put(Bytes.toBytes(bjRowKey));
        putB.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("call2"),Bytes.toBytes(call2));
        putB.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("call1"),Bytes.toBytes(call1));
        putB.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        putB.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("duration"),Bytes.toBytes(duration));
        putB.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("flag"),Bytes.toBytes(ValConstant.NUM_BJ));

        putList.add(putB);
        putData(Names.TABLE.getValue(),putList);

        //此方式造成：一条数据两个put对象，客户端发送两个插入数据请求，效率差，使用协处理器方式
    }

    /**
     * 插入数据
     * @param log 数据对象
     * @throws IOException
     * @throws IllegalAccessException
     */
    public void insertData(Calllog log) throws IOException, IllegalAccessException {
        //设置key
        log.setRowKey(getRegionNum(log.getCall1(), log.getCalltime()) + "_" + log.getCall1() + "_" + log.getCalltime() + "_" + log.getCall2() + "_" + log.getDuration()+"_"+log.getFlag());
        putData(log);
    }


    public static void main(String[] args) {
        System.out.println();
    }
}

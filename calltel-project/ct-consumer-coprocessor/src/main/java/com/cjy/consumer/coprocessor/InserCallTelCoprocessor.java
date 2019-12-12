package com.cjy.consumer.coprocessor;


import com.cjy.ct.bean.BaseDao;
import com.cjy.ct.constant.Names;
import com.cjy.ct.constant.ValConstant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 插入数据协处理器保存被叫用户
 *
 * 协处理器的使用
 * 1。创建类
 * 2。继承BaseRegionObserver类
 * 3。重写postPut，业务逻辑
 * 4。将协处理器全类名与创建要使用的表关联起来
 * 5。打包成jar包，发布到hbase中，关联jar包也需要发布common包，并且分发，因为集群环境不一定是在那台机器上操作，hbase/lib
 */
public class InserCallTelCoprocessor extends BaseRegionObserver {
    /**
     * 插入数据后
     * @param e    环境对象，可获取表对象
     * @param put  插入数据封装的对象
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //从环境中获取表对象
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));
        //put中获取主叫rowkey
        String rowKey = Bytes.toString(put.getRow());

        //由于rowkey保存了所有数据，所以解析rowkey
        String[] values = rowKey.split("_");
        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];
        String flag = values[5];

        //注意：只有标记是0的才是主叫数据，主数据才能解析处理被叫数据，不然会陷入死循环，插入一条同步一条。
        if("0".equals(flag)){

            //获取分区号，封装数据
            CoprocessorDao dao = new CoprocessorDao();
            Integer regionNum = dao.getRegionNum(call2, calltime);
            String bjRowKey = regionNum+"_"+call2+"_"+calltime+"_"+call1+"_"+duration+"_"+ ValConstant.NUM_BJ;

            Put put1 = new Put(Bytes.toBytes(bjRowKey));
            put1.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("call1"),Bytes.toBytes(call2));
            put1.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("call2"),Bytes.toBytes(call1));
            put1.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
            put1.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("duration"),Bytes.toBytes(duration));
            put1.addColumn(Bytes.toBytes(Names.CF_CALLEE.getValue()),Bytes.toBytes("flag"),Bytes.toBytes(ValConstant.NUM_BJ));

            table.put(put1);
            table.close();
        }
    }

    private class CoprocessorDao extends BaseDao{
        @Override
        protected Integer getRegionNum(String tel, String date) {
            return super.getRegionNum(tel, date);
        }
    }

    public static void main(String[] args) {
        System.out.println(InserCallTelCoprocessor.class.getName());
    }
}

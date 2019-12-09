package com.cjy.consumer.dao;

import com.cjy.ct.bean.BaseDao;
import com.cjy.ct.constant.Names;

import java.io.IOException;

public class HbaseDao extends BaseDao {

    /**
     * 初始化建表空间与表
     * @throws IOException
     */
    public void init() throws IOException {
        start();
        createNamespaceNX(Names.NAMESTPACE.getValue());
        createTalbeXX(Names.TABLE.getValue(),Names.CF_CALLER.getValue());
        end();
    }

    /**
     * 数据插入
     * @param value
     */
    public void insertData(String value){
        // 1. 获取通话日志数据
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        //
    }
}

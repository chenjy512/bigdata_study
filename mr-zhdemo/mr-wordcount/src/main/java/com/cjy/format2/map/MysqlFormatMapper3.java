package com.cjy.format2.map;

import com.cjy.format2.bean.DataBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MysqlFormatMapper3 extends Mapper<LongWritable, DataBean, Text, DataBean> {

    Text keystr = new Text();

    @Override
    protected void map(LongWritable key, DataBean value, Context context) throws IOException, InterruptedException {
            //需要使用key来分组数据
            keystr.set(value.getDwmc()+"_"+value.getCode());
            context.write(keystr,value);
    }
}

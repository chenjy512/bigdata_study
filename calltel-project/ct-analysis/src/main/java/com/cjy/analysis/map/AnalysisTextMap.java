package com.cjy.analysis.map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;


import java.io.IOException;

public class AnalysisTextMap extends TableMapper<Text,Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//      5_16480981069_20180830214723_18749966182_0682_0   解析数据
        //1. 拆分数据
        String s = Bytes.toString(key.get());
//        System.out.println("map:"+s);
        String[] values = s.split("_");

        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];

        //2. 编辑条件，也就是key
        String year = calltime.substring(0,4);
        String month = calltime.substring(0,6);
        String day = calltime.substring(0,8);

        //3. 数据写出，通话是两个人的事情，所以主叫被叫都应该计算通话时长
        context.write(new Text(call1+"_"+year),new Text(duration));
        context.write(new Text(call1+"_"+month),new Text(duration));
        context.write(new Text(call1+"_"+day),new Text(duration));
        //被叫数据
        context.write(new Text(call2+"_"+year),new Text(duration));
        context.write(new Text(call2+"_"+month),new Text(duration));
        context.write(new Text(call2+"_"+day),new Text(duration));
    }

    public static void main(String[] args) {
        String calltime = "20180830214723";
        String year = calltime.substring(0,4);
        String month = calltime.substring(0,6);
        String day = calltime.substring(0,8);
        System.out.println(year);
        System.out.println(month);
        System.out.println(day);
    }
}
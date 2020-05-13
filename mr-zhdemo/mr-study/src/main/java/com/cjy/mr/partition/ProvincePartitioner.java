package com.cjy.mr.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {

        //获取输入key
        String s = text.toString();
        //判断人名
        if("zhangsan".equals(s)){
            return 0;
        }else if("lisi".equals(s)){
            return 1;
        }else{
            return 2;
        }
    }
}

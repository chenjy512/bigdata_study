package com.cjy.format.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MysqlFormatMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    Text keys = new Text();
    Text val = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String k =split[1] + "_"+split[2];
        keys.set(k);
        val.set(split[3]);
        context.write(keys,val);
    }
}

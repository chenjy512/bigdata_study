package com.cjy.mr.nline;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    // 输出数据格式，也就是reduce接受的格式
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取当前行文本
        String line = value.toString();

        //按照 \t 分割字符
        String[] words = line.split("\t");

        //循环将每个字符写出，reduce会按照相同key的数据聚集到一起
        for (String word : words) {
            k.set(word);
            //word  1
            //java  1
            context.write(k,v);
        }
    }
}
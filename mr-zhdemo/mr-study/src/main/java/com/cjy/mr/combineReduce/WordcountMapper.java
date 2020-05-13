package com.cjy.mr.combineReduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 编写wordcount map类：完成数据输入
 *
 * FileInputFormat默认实现是TextInputFormat，所以是一行一行读入数据，key是偏移量，v是读取行字符串
 */
public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

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

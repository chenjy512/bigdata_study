package com.cjy.mrtest;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * mapper什么都不做将数据读入进来，写给reduce做处理
 */
public class FruitMapper
        extends Mapper<LongWritable, Text, LongWritable, Text>
{
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
            throws IOException, InterruptedException
    {
        context.write(key, value);
    }
}

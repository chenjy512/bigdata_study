package com.cjy.hbase.mr1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper什么都不做将数据读入进来，写给reduce做处理
 */
public class FruitMapper
        extends Mapper<LongWritable, Text, LongWritable, Text>
{
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        context.write(key, value);
    }
}

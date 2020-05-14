package com.cjy.mr.mysqlformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MysqlMapper extends Mapper<Order,NullWritable,Order,NullWritable> {
    @Override
    protected void map(Order key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
        }
}

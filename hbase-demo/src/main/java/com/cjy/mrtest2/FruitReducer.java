package com.cjy.mrtest2;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer：这里就不能继承原生的reduce，由于将数据写入hbase所以需要继承hbaes下的reduce import org.apache.hadoop.hbase.mapreduce.TableReducer;

 */
public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable>
{
    protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text,
            NullWritable, Mutation>.Context context)
            throws IOException, InterruptedException
    {
        for (Text value : values)
        {
            //1. 分割数据：1001  Apple	Red
            String[] fields = value.toString().split("\t");
            //2. 创建put对象，设置key
            Put put = new Put(Bytes.toBytes(fields[0]));
            //3. 封装多列数据
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));
            //4. 写出数据
            context.write(NullWritable.get(), put);
        }
    }
}
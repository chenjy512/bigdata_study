package com.cjy.mrtest;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FruitReducer
        extends TableReducer<LongWritable, Text, NullWritable>
{
    protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, NullWritable, Mutation>.Context context)
            throws IOException, InterruptedException
    {
        for (Text value : values)
        {
            String[] fields = value.toString().split("\t");

            Put put = new Put(Bytes.toBytes(fields[0]));

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));

            context.write(NullWritable.get(), put);
        }
    }
}
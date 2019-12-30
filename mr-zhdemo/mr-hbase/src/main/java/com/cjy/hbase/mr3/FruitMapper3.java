package com.cjy.hbase.mr3;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 直接读取数据，写出即可
 */
public class FruitMapper3 extends TableMapper<Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //1_apple_red
        String s = Bytes.toString(key.get());
        k.set(s);
        v.set(s);
        context.write(k,v);
    }
}

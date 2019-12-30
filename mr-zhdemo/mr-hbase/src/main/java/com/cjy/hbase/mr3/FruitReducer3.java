package com.cjy.hbase.mr3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 直接写出数据
 */
public class FruitReducer3 extends Reducer<Text,Text,Text,Text> {
    Text v = null;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()){
             v = iterator.next();
        }
        context.write(key,v);
    }
}

package com.cjy.mr.inputformat1;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算每个文件多少单词
 */
public class WholeFileReducer extends Reducer<Text, BytesWritable, Text, LongWritable> {
    LongWritable v = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context)		throws IOException, InterruptedException {

        Iterator<BytesWritable> iterator = values.iterator();

        while(iterator.hasNext()){

            BytesWritable next = iterator.next();
            //字节数据转为字符
            String content = new String(next.getBytes(), 0, next.getLength());
            //字符切割
            String[] split = content.split("\t");
            v.set(split.length);
        }
        //数据写出
        context.write(key, v);
    }
}
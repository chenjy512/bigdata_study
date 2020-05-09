package com.cjy.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 编写reducer类：计算相同单词出现的次数
 * 从map接受数据类型如下：
 *
 * （java,(1,1,1,1)）
 */
public class WordcountReducer extends Reducer<Text,IntWritable,Text, IntWritable> {

    int sum ;
    IntWritable  v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }

        // 2 输出
        v.set(sum);
        //java  5
        //hello 4
        context.write(key,v);
    }
}

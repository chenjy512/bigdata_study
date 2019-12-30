package com.cjy.format.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MysqlFormatReducer2 extends Reducer<Text, Text,Text, Text> {

    Text val = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        int sum = 0;
        Iterator<Text> iterator = values.iterator();
        while(iterator.hasNext()){
            String s = iterator.next().toString();
            count++;
            sum += Integer.parseInt(s);
        }
        val.set(count+"_"+sum);
        context.write(key,val);
    }
}

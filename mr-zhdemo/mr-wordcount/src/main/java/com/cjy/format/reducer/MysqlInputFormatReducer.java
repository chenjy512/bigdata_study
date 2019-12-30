package com.cjy.format.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MysqlInputFormatReducer extends Reducer<Text, Text,Text,Text> {
    Text val = null;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while(iterator.hasNext()){
            val = iterator.next();
        }
        context.write(key,val);
    }

    public static void main(String[] args) {
        System.out.println(1/2);
    }
}

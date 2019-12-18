package com.cjy.analysis.reducer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AnalysisTextReduce extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

       int sum=0;
       int count=0;
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()){
            String s = iterator.next().toString();
            int duration = Integer.parseInt(s);
            sum += duration;
            count++;
        }
//        System.out.println(key.toString()+"\t"+count+"\t"+sum);
        context.write(key,new Text(count+"_"+sum));
    }
}

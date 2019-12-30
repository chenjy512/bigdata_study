package com.cjy.format2.reducer;

import com.cjy.format2.bean.DataBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MysqlFormatReducer3 extends Reducer<Text, DataBean,Text, Text> {

    Text val = new Text();

    @Override
    protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
        Iterator<DataBean> iterator = values.iterator();
        int count = 0;
        int sum = 0;
        while (iterator.hasNext()){
            DataBean next = iterator.next();
            count++;
            sum += Integer.parseInt(next.getValue());
        }
        val.set(count+"_"+sum);
        context.write(key,val);
    }
}

package com.cjy.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProductReducer extends Reducer< Product,Text,Text, Product> {


    Product v = new Product();

    @Override
    protected void reduce(Product key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {

        for (Text value : values) {
            context.write(value,key);
        }
    }
}

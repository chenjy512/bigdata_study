package com.cjy.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<LongWritable, Text,Product, Text> {

    Product product = new Product();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据
        String lines = value.toString();
        //分割
        String[] s = lines.split("\t");
        //同一人同一种商品拼接
        String keystr = s[0]+"_"+s[1];
        k.set(keystr);

        //  lisi_A1 10  76
        product.setInfo(Long.parseLong(s[2]),Integer.parseInt(s[3]));

        context.write(product,k);
    }
}

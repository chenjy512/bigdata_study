package com.cjy.mr.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProductReducer extends Reducer<Text,Product,Text,Product> {

    long sum;   //总额
    long sumCount;  //总数量
    int price;  //单价
    Product v = new Product();

    //接收数据格式:lisi_A1 10  76
    @Override
    protected void reduce(Text key, Iterable<Product> values, Context context) throws IOException, InterruptedException {
        //每次调用初始化数据
        sum = 0;
        sumCount = 0;
        price = 0;

        //计算每个人每种商品的总量、总额
        for (Product value : values) {
            //获取单价数量
            long count = value.getCount();
            price = value.getPrice();
            //计算总数量
            sumCount+=count;
            //计算总额
            sum+=(count*price);
        }
        //写出数据封装
        v.setInfo(sumCount,price);
        v.setSum(sum);
        //写出
        context.write(key,v);

    }

}

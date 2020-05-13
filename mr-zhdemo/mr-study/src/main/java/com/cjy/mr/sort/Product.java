package com.cjy.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Product implements WritableComparable<Product> {

     private long count;     //数量
     private int price;     //价格
     private long sum;      //总额

    public Product() {
    }


    public void setInfo(long count,int price){

        this.count=count;
        this.price=price;
        this.sum = count*price;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
                         out.writeLong(count);
                         out.writeInt(price);
                         out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
                         count =  in.readLong();
                         price = in.readInt();
                         sum = in.readLong();
    }

    @Override
    public String toString() {
        return count + "\t" +price + "\t"+ sum ;
    }

    /**
     * 比较函数
     * @param o
     * @return
     */
    @Override
    public int compareTo(Product o) {
        //降序操作
        if (sum > o.getSum()){
            return -1;
        }else{
            return 1;
        }
    }
}

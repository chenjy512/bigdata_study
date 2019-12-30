package com.cjy.analysis.kvbean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1。自定义key需要实现WritableComparable，用于比较key分组
 * 2。需要有空参构造器，来反射创建对象
 * 3。重写比较规则
 * 4。重写输出，读入函数
 */
public class AnalysisKey implements WritableComparable<AnalysisKey> {

    private String tel;
    private String date;

    public AnalysisKey() {
    }

    public AnalysisKey(String tel, String date) {
        this.tel = tel;
        this.date = date;
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    /**
     * 比较:tel, date
     * @param key
     * @return
     */
    public int compareTo(AnalysisKey key) {
        int result = tel.compareTo(key.getTel());

        if ( result == 0 ) {
            result = date.compareTo(key.getDate());
        }

        return result;
    }

    /**
     * 写数据
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(tel);
        out.writeUTF(date);
    }

    /**
     * 读数据
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        tel = in.readUTF();
        date = in.readUTF();
    }
}

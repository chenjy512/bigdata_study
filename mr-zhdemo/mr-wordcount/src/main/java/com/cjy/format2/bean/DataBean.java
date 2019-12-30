package com.cjy.format2.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataBean implements WritableComparable<DataBean>,Writable{

    private String id;
    private String dwmc;
    private String code;
    private String value;

    public DataBean() {

    }

    public DataBean(String id, String dwmc, String code, String value) {
        this.id = id;
        this.dwmc = dwmc;
        this.code = code;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDwmc() {
        return dwmc;
    }

    public void setDwmc(String dwmc) {
        this.dwmc = dwmc;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return id+"\t"+dwmc + "\t"+ code + "\t" + value;
    }


    /**
     * write -- read ：先进先出
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(dwmc);
        out.writeUTF(code);
        out.writeUTF(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.dwmc = in.readUTF();
        this.code = in.readUTF();
        this.value = in.readUTF();
    }

    @Override
    public int compareTo(DataBean o) {
        int res = dwmc.compareTo(o.dwmc);
        if(res == 0){
            res = code.compareTo(o.code);
        }
        return res;
    }
}

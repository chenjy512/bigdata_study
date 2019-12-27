package com.cjy.format.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

public class DataBean implements Comparator<DataBean>,Writable{

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


    @Override
    public int compare(DataBean o1, DataBean o2) {
      if(o1.code.equals(o2.code)){
          return 0;
      }else{
          return 1;
      }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(id);
        out.writeBytes(dwmc);
        out.writeBytes(code);
        out.writeBytes(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
//        this.id = in.readByte()
    }
}

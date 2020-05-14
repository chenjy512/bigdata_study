package com.cjy.mr.mysqlformat;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order implements WritableComparable<Order> {

    private String id;
    private String userCode;
    private String productCode;
    private Integer value;

    public Order() {

    }

    public Order(String id, String userCode, String productCode, Integer value) {
        this.id = id;
        this.userCode = userCode;
        this.productCode = productCode;
        this.value = value;
    }


    public void setInfo(String id, String userCode, String productCode, Integer value){
        this.id = id;
        this.userCode = userCode;
        this.productCode = productCode;
        this.value = value;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserCode() {
        return userCode;
    }

    public void setUserCode(String userCode) {
        this.userCode = userCode;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }



    //排序
    @Override
    public int compareTo(Order o) {
        int res=0;
        if(userCode.compareTo(o.getUserCode()) > 0){
            res= 1;
        }else if(userCode.compareTo(o.getUserCode()) < 0){
            res= -1;
        }else{
            if(value > o.getValue()){
                res= 1;
            }
        }
        return res;
    }

    //序列化操作
    @Override
    public void write(DataOutput out) throws IOException {
            out.writeUTF(id);
            out.writeUTF(userCode);
            out.writeUTF(productCode);
            out.writeInt(value);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        userCode = in.readUTF();
        productCode = in.readUTF();
        value = in.readInt();
    }

    @Override
    public String toString() {
        return id+"\t"+userCode+"\t"+productCode+"\t"+value;
    }
}

package com.cjy.consumer.bean;

import com.cjy.ct.api.Column;
import com.cjy.ct.api.RowKey;
import com.cjy.ct.api.TableRef;
import com.cjy.ct.constant.Names;
import com.cjy.ct.constant.ValConstant;

//注意注解中不能使用枚举
//@TableRef(value = Names.TABLE.getValue())
@TableRef(value = ValConstant.TABLE_NAME)
public class Calllog {

    @RowKey
    private String rowKey;
    //列名也就是属性名不指定可以使用默认
    @Column(family = ValConstant.FAMILY_CF1,column = "call1")
    private String call1;
    @Column(family = ValConstant.FAMILY_CF1)
    private String call2;
    @Column(family = ValConstant.FAMILY_CF1)
    private String calltime;
    @Column(family = ValConstant.FAMILY_CF1)
    private String duration;


    public Calllog(String data ) {
        String[] values = data.split("\t");
        call1 = values[0];
        call2 = values[1];
        calltime = values[2];
        duration = values[3];
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getCall1() {
        return call1;
    }

    public void setCall1(String call1) {
        this.call1 = call1;
    }

    public String getCall2() {
        return call2;
    }

    public void setCall2(String call2) {
        this.call2 = call2;
    }

    public String getCalltime() {
        return calltime;
    }

    public void setCalltime(String calltime) {
        this.calltime = calltime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }
}

package com.cjy.produce.bean;

import com.cjy.ct.bean.Data;

public class Contact extends Data {

    private String tel;
    private String name;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setValue(Object val) {
        content = (String) val;
        String[] split = content.split("\t");
        this.tel = split[0];
        this.name = split[1];
    }

    @Override
    public String toString() {
        return "[tel: "+tel+" , name: "+name+"]";
    }
}

package com.cjy.ct.constant;

/**
 * 枚举常量
 */
public enum Names {

    TOPIC("calllog")
    ,NAMESTPACE("ct")
    ,CF_CALLER("caller")
    ,CF_CALLEE("callee")
    ,CF_INFO("info")
    ,TABLE("ct:calllog");

   private String value;
    /**
     * 构造函数
     * @param val
     */
    private Names(String val){
        this.value = val;
    }

    public void setValue(Object val){
        this.value = (String) val;
    }
    //返回值
    public String getValue(){
        return value;
    }
}

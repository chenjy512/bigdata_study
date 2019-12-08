package com.cjy.ct.bean;

public interface Producer {

    /**
     * 读入数据
     * @param dataIn
     */
    public void setDataIn(DataIn dataIn);

    /**
     * 读出数据
     * @param dataOut
     */
    public void setDataOut(DataOut dataOut);

    /**
     * 生产数据
     */
    public void produce();


    /**
     * 根据读入数据处理加工后输出
     */
}

package com.cjy.analysis.kvbean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  * 1。自定义key需要实现 Writable
 *  * 2。需要有空参构造器，来反射创建对象
 *  * 3。重写输出，读入函数
 */
public class AnalysisValue implements Writable {

    private Integer sumCall;
    private Integer sumDuration;

    public AnalysisValue() {
    }

    public AnalysisValue(Integer sumCall, Integer sumDuration) {
        this.sumCall = sumCall;
        this.sumDuration = sumDuration;
    }

    public Integer getSumCall() {
        return sumCall;
    }

    public void setSumCall(Integer sumCall) {
        this.sumCall = sumCall;
    }

    public Integer getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Integer sumDuration) {
        this.sumDuration = sumDuration;
    }

    /**
     * 写数据
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(sumCall+"");
        out.writeUTF(sumDuration+"");
    }

    /**
     * 读数据
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        sumCall = Integer.parseInt(in.readUTF());
        sumDuration = Integer.parseInt(in.readUTF());
    }
}

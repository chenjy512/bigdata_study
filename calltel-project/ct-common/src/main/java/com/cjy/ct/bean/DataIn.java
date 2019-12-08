package com.cjy.ct.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 数据读入对象
 */
public interface DataIn extends Closeable {

    public void setPath(String path);
    public Object read();
    public <T extends Data> List<T> read(Class<T> tClass) throws IOException;
}

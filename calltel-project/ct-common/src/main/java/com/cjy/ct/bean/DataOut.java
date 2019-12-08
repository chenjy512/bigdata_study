package com.cjy.ct.bean;

import java.io.Closeable;

public interface DataOut extends Closeable {

    public void setPath(String path);
    public void write(Object data);
    public void write(String data);
}

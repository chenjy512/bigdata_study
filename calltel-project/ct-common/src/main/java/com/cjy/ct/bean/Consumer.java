package com.cjy.ct.bean;

import java.io.Closeable;
import java.io.IOException;

public interface Consumer extends Closeable {

    /**
     * 消费方法
     */
    public void consume();
}

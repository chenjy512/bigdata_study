package com.cjy.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * 1.实现flume拦截器接口，重写接口定义函数
 */
public class CustomInterceptor implements Interceptor {
    //初始化函数
    @Override
    public void initialize() {

    }
    //单个事件处理
    @Override
    public Event intercept(Event event) {
        //获取数据字节数组
        byte[] body = event.getBody();
        //转为字符串
        String s = new String(body);
        //判断数据是否包含 order 字符，event设置头信息，供selector选择对应channel
        if(s.indexOf("order") != -1){
            event.getHeaders().put("type", "order");
        }else{
            event.getHeaders().put("type", "other");
        }
        return event;
    }

    /**
     * 处理一组事件，调用重载函数处理
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event:list) {
            //单事件处理
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    //自定义拦截器对象创建方式
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            //构建对象
            return new CustomInterceptor();
        }
        @Override
        public void configure(Context context) {

        }
    }
}

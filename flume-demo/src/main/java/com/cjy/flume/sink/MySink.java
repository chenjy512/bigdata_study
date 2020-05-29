package com.cjy.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink
        extends AbstractSink
        implements Configurable {
    private Logger logger = LoggerFactory.getLogger(MySink.class);
    private String prefix;
    private String suffix;

    public void configure(Context context) {
        //读取配置文件内容，有默认值
        this.prefix = context.getString("prefix", "hello:");
        //读取配置文件内容，无默认值
        this.suffix = context.getString("suffix");
    }

    public Sink.Status process()
            throws EventDeliveryException { //声明返回值状态信息
        Sink.Status status = null;
        //获取当前 Sink 绑定的 Channel
        Channel channel = getChannel();
        //获取事务
        Transaction transaction = channel.getTransaction();
        //声明事件
        Event event;
        //开启事务
        transaction.begin();
        try {
            //读取 Channel 中的事件，直到读取到事件结束循环
            while (true) {
                event = channel.take();
                if (event != null) {
                    break;
                }
            }

            if (event != null) {
                String body = new String(event.getBody());
                //处理事件（打印）
                this.logger.info(this.prefix + body + this.suffix);
            }
            //事务提交
            transaction.commit();
            status = Sink.Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            //遇到异常，事务回滚
            transaction.rollback();
            status = Sink.Status.BACKOFF;
        } finally {
            //关闭事务
            transaction.close();
        }
        return status;
    }
}

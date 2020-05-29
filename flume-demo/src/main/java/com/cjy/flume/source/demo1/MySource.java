package com.cjy.flume.source.demo1;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class MySource
        extends AbstractSource
        implements Configurable, PollableSource
{
    private String prefix;
    private String suffix;

    public void configure(Context context)
    {
        this.prefix = context.getString("prefix");
        this.suffix = context.getString("subfix", "ccjy");
    }

    public PollableSource.Status process()
            throws EventDeliveryException
    {
        PollableSource.Status status = null;
        try
        {
            //创建事件
            SimpleEvent event = new SimpleEvent();
            //创建事件头信息
            HashMap<String, String> hearderMap = new HashMap<>();
            for (int i = 0; i < 5; i++)
            {
                //给事件设置头信息
                event.setHeaders(hearderMap);
                //给事件设置内容
                event.setBody((this.prefix + "--" + i + "--" + this.suffix).getBytes());
                //将事件写入 channel
                getChannelProcessor().processEvent(event);

                status = PollableSource.Status.READY;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            //异常回滚状态
            status = PollableSource.Status.BACKOFF;
        }
        try
        {
            Thread.sleep(2000L);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        return status;
    }

    public long getBackOffSleepIncrement()
    {
        return 0L;
    }

    public long getMaxBackOffSleepInterval()
    {
        return 0L;
    }
}

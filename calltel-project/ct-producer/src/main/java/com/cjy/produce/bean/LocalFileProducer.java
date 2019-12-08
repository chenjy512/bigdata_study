package com.cjy.produce.bean;

import com.cjy.ct.bean.DataIn;
import com.cjy.ct.bean.DataOut;
import com.cjy.ct.bean.Producer;
import com.cjy.ct.util.DateUtil;
import com.cjy.ct.util.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class LocalFileProducer implements Producer {

    private DataIn in;
    private DataOut out;
    private volatile boolean flag = true;

    public void setDataIn(DataIn dataIn) {
            this.in=dataIn;
    }

    public void setDataOut(DataOut dataOut) {
            this.out = dataOut;
    }

    public void produce() {

        try {
            List<Contact> read = in.read(Contact.class);
//            for (Contact contact : read) {
//                System.out.println(contact);
//            }
             while(flag){
                 int callIndex1 = new Random().nextInt(read.size());
                 int callIndex2;
                 while(true){
                     callIndex2 = new Random().nextInt(read.size());
                     if(callIndex1 != callIndex2){
                         break;
                     }
                 }

                 Contact call1 = read.get(callIndex1);
                 Contact call2 = read.get(callIndex2);

                // 生成随机的通话时间
                 String startDate = "20180101000000";
                 String endDate = "20190101000000";

                 long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                 long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

                 // 通话时间
                 long calltime = startTime + (long)((endTime - startTime) * Math.random());
                 // 通话时间字符串
                 String callTimeString = DateUtil.format(new Date(calltime), "yyyyMMddHHmmss");

                 // 生成随机的通话时长
                 String duration = NumberUtil.format(new Random().nextInt(3000), 4);

                 // 生成通话记录
                 Calllog log = new Calllog(call1.getTel(), call2.getTel(), callTimeString, duration);

                 System.out.println(log);
                 // 将通话记录刷写到数据文件中
                 out.write(log);

                 try {
                     Thread.sleep(500);
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 }
             }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void close() {
        if(in != null){
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(out != null ){
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

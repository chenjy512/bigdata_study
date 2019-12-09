package com.cjy.produce;

import com.cjy.produce.bean.LocalFileProducer;
import com.cjy.produce.io.LocalFileDataIn;
import com.cjy.produce.io.LocalFileDataOut;

public class BootStrap {
    public static void main(String[] args) {
        System.out.println("生产者启动类，开始生产数据。。。");
//        if(args.length <2){
//            System.out.println("jar 运行参数格式不正确，   java -jar xx.jar /inpath /outpath ");
//            System.exit(1);
//        }

        LocalFileProducer producer = new LocalFileProducer();
//        producer.setDataIn(new LocalFileDataIn("/Users/chenjunying/Downloads/contact.log"));
//        producer.setDataOut(new LocalFileDataOut("/Users/chenjunying/Downloads/calllog.log"));
        producer.setDataIn(new LocalFileDataIn("D:\\temp\\contact.log"));
        producer.setDataOut(new LocalFileDataOut("D:\\temp\\calllog.log"));
//        producer.setDataIn(new LocalFileDataIn(args[0]));
//        producer.setDataOut(new LocalFileDataOut(args[1]));
        producer.produce();

    }
}

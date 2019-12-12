package com.cjy.consumer;

import com.cjy.consumer.bean.KafkaCalllogConsumer;

import java.io.IOException;

public class Bootstrap {
    public static void main(String[] args) throws IOException {
        /**
         * 创建消费者对象，消费数据
         */
        KafkaCalllogConsumer consumer = new KafkaCalllogConsumer();
        consumer.consume();
        consumer.close();
    }
}

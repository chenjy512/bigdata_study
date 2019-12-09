package com.cjy.consumer;

import com.cjy.consumer.bean.KafkaCalllogConsumer;

import java.io.IOException;

public class Bootstrap {
    public static void main(String[] args) throws IOException {
        KafkaCalllogConsumer consumer = new KafkaCalllogConsumer();
        consumer.consume();
        consumer.close();
    }
}

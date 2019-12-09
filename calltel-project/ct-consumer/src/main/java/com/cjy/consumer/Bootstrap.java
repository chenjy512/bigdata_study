package com.cjy.consumer;

import com.cjy.consumer.bean.KafkaCalllogConsumer;

public class Bootstrap {
    public static void main(String[] args) {
        KafkaCalllogConsumer consumer = new KafkaCalllogConsumer();
        consumer.consume();
    }
}

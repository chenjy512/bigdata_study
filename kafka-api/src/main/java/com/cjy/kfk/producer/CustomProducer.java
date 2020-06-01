package com.cjy.kfk.producer;

import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomProducer {

    private static Properties p = null;
    private static String TOPIC="order_info";
    static{
         p = new Properties();
        try {
            p.load(CustomProducer.class.getClassLoader().getResourceAsStream("jdbc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 测试不带回调函数
     */
    public static void producerClient1(){
        Properties props = new Properties();
        //kafka 集群，broker-list
        props.put("bootstrap.servers", "hadoop202:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new
                KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("order_info",
                    Integer.toString(i), Integer.toString(i)+"_"));
        }
        producer.close();
    }

    //加载配置文件方式
    public static void producerClient2() {
        Producer<String, String> producer = new KafkaProducer<>(p);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("order_info",
                    Integer.toString(i), Integer.toString(i)+"_i"));
        }
        producer.close();
    }


    //带回调函数的 API
    public static void producerClient3() {
        Producer<String, String> producer = new
                KafkaProducer<>(p);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>(TOPIC,
                    Integer.toString(i), Integer.toString(i)), new Callback() {
                //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata,
                                         Exception exception) {
                    if (exception == null) {
                        System.out.println("success->" +
                                metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }

    /**
     * 同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回 ack。
     */
    public static void producerClient4() {
        Producer<String, String> producer = new
                KafkaProducer<>(p);
        for (int i = 0; i < 10; i++) {
            try {
                producer.send(new ProducerRecord<String, String>(TOPIC,
                        Integer.toString(i), Integer.toString(i))).get();
            } catch  (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    /**
     * 自定义分区器
     */
    public static void producerClient5() {
        p.put("partitioner.class","com.cjy.kfk.partitioner.MyPartitioner");
        Producer<String, String> producer = new
                KafkaProducer<>(p);
        for (int i = 0; i < 10; i++) {
            try {
                producer.send(new ProducerRecord<String, String>(TOPIC,
                        Integer.toString(i), Integer.toString(i)),new Callback() {
                    //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                    @Override
                    public void onCompletion(RecordMetadata metadata,
                                             Exception exception) {
                        if (exception == null) {
                            System.out.println("partition->"+metadata.partition()+"    offset->" +
                                    metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    }
                });
            } catch  (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
    public static void main(String[] args) {
//        producerClient1();
//        producerClient2();
//        producerClient3();
//        producerClient4();
        producerClient5();
    }
}

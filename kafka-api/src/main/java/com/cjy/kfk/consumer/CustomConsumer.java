package com.cjy.kfk.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class CustomConsumer {

    private static Properties props =null;

    static{
        //配置信息
        props = new Properties();
        props.put("bootstrap.servers", "hadoop202:9092");
        props.put("group.id", "test");

        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static void consumerTest1(){
        props.put("group.id", "test1");
        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
        //重置offset，读取所有数据
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //创建客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Arrays.asList("order_info"));
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    //同步提交 offset：由于同步提交 offset 有失败重试机制，故更加可靠
    public static void consumerTest2(){
        //Kafka 集群
        props.put("enable.auto.commit", "false");//关闭自动提交 offset
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("order_info"));//消费者订阅主题
        while (true) {
            //消费者拉取数据
            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value= %s%n", record.offset(), record.key(), record.value());
            }
            //同步提交，当前线程会阻塞直到 offset 提交成功
            consumer.commitSync();
        }

    }

    //异步提交 offset
    public static void consumerTest3(){
        //Kafka 集群
        props.put("enable.auto.commit", "false");//关闭自动提交 offset
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("order_info"));//消费者订阅主题
        while (true) {
            //消费者拉取数据
            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value= %s%n", record.offset(), record.key(), record.value());
            }
            //异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition,
                                        OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.err.println("Commit failed for" +
                                offsets);
                    }
                }
            });
        }

    }
    public static void main(String[] args) {
        consumerTest1();
//        consumerTest2();
//        consumerTest3();
    }
}

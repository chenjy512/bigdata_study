package com.cjy.consumer.bean;

import com.cjy.ct.bean.Consumer;
import com.cjy.ct.bean.Data;
import com.cjy.ct.constant.Names;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaCalllogConsumer implements Consumer{


    @Override
    public void consume() {
        try {
            //加载kafka配置文件
            Properties prop = new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
            //创建kafka消费者
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(prop);
            //设置消费主题
            kafkaConsumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

            while (true){
                //一次拿去多条数据
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(100);
                Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
                while (iterator.hasNext()){
                    ConsumerRecord<String, String> next = iterator.next();
                    System.out.println(next.value());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void close() throws IOException {

    }
}

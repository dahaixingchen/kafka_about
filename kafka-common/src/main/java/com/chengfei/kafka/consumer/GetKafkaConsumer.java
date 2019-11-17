package com.chengfei.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @ClassName: GetKafkaConsumer
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/16 21:15
 * @Version 1.0
 **/
public class GetKafkaConsumer {
    public static Consumer getKafkaConsumer(){
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node-1:9092,node-2:9092,node-3:9092");
        pro.put(ConsumerConfig.GROUP_ID_CONFIG,"kafkaConsumer");
        return null;
    }
}

package com.feifei.ack;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * @ClassName: MyKafkaConsumer
 * @Author chengfei
 * @Date 2020/11/28 21:35
 * @Description: TODO 普通消费者代码
 **/
public class MyKafkaConsumer {
    public static void main(String[] args) {
        Properties pro = new Properties();
//        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"emr-header-1:9092,emr-worker-1:9092,emr-worker-2:9092");
        pro.put(ConsumerConfig.GROUP_ID_CONFIG,"g44");
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest"); //earliest,latest
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(pro);

        //所有的分区都会消费到
        kafkaConsumer.subscribe(Arrays.asList("test"));
        //订阅特定的分区
//        kafkaConsumer.assign(Collections.singleton(new TopicPartition("topic", 0)));

        while (true){
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            while (iterator.hasNext()){
                ConsumerRecord<String, String> record = iterator.next();
                System.out.println("key:" + record.key() + "---value:" + record.value()
                        + "---partition:" + record.partition() + "----offset" + record.offset());
            }
        }

    }
}

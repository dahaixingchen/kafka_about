package com.feifei.offset;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @ClassName: KafkaOffsetHand
 * @Author chengfei
 * @Date 2020/11/29 10:37
 * @Description: TODO 手动设定固定的offset值开始消费
 **/
public class KafkaSetOffset {
    public static void main(String[] args) {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        pro.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"zstd");

//        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        pro.put(ConsumerConfig.GROUP_ID_CONFIG,"g6");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(pro);

        kafkaConsumer.subscribe(Arrays.asList("topic"));

        Set<TopicPartition> assignment = new HashSet<>();
//        while (assignment.size() == 0){
            //在assignment一定要先poll一下kafka的数据
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment =  kafkaConsumer.assignment();
//        }

        for (TopicPartition topicPartition : assignment) {
            kafkaConsumer.seek(topicPartition,6);
        }

        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            Iterator<ConsumerRecord<String, String>> datas = records.iterator();
            while (datas.hasNext()){
                ConsumerRecord<String, String> record = datas.next();
                System.out.println("key:" + record.key() + "---value:" + record.value()
                        + "---partition:" + record.partition() + "----offset" + record.offset());
            }

        }
    }
}

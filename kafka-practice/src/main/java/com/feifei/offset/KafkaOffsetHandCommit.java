package com.feifei.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @ClassName: KafkaOffsetAutoCommit
 * @Author chengfei
 * @Date 2020/11/29 10:37
 * @Description: TODO 手动提交offset到集群中
 **/
public class KafkaOffsetHandCommit {
    public static void main(String[] args) {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        pro.put(ConsumerConfig.GROUP_ID_CONFIG,"g7");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(pro);

//        kafkaConsumer.assign(Arrays.asList(new TopicPartition("topic",0)));
        kafkaConsumer.subscribe(Arrays.asList("topic"));

        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
            while (recordIterator.hasNext()){
                ConsumerRecord<String, String> record = recordIterator.next();

                Map<TopicPartition, OffsetAndMetadata> hashMap = new HashMap<>();
                hashMap.put(new TopicPartition("topic",0)
                        ,new OffsetAndMetadata(record.offset() + 1));
                OffsetCommitCallback offsetCommitCallback =  new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

                    }
                };
                kafkaConsumer.commitAsync(hashMap,offsetCommitCallback);
                System.out.println("key:" + record.key() + "---value:" + record.value()
                        + "---partition:" + record.partition() + "----offset" + record.offset());
            }
        }
    }
}

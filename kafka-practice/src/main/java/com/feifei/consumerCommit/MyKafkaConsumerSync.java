package com.feifei.consumerCommit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName: MyKafkaConsumer
 * @Author chengfei
 * @Date 2020/11/28 21:35
 * @Description: TODO 同步提交offset
 **/
public class MyKafkaConsumerSync {
    public static void main(String[] args) {
        Properties pro = new Properties();
//        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"emr-header-1:9092,emr-worker-1:9092,emr-worker-2:9092");
        pro.put(ConsumerConfig.GROUP_ID_CONFIG,"g4");
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //设置offset手动提交
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,false);
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //earliest,latest
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(pro);

        //所有的分区都会消费到
        kafkaConsumer.subscribe(Arrays.asList("test"));
        //订阅特定的分区
//        kafkaConsumer.assign(Collections.singleton(new TopicPartition("topic", 0)));

        while (true){
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            long i = 0;
            while (iterator.hasNext()){
                i++;
                ConsumerRecord<String, String> record = iterator.next();
                System.out.println("key:" + record.key() + "---value:" + record.value()
                        + "---partition:" + record.partition() + "----offset" + record.offset());
            }
            //设置offset同步提交，
//            kafkaConsumer.commitSync();

            //按时间提交
//            kafkaConsumer.commitSync(Duration.ofSeconds(1));

            //更加细粒度的提交
            Map<TopicPartition, OffsetAndMetadata> topicMap = new HashMap<>();
            topicMap.put(new TopicPartition("test",0),new OffsetAndMetadata(i));
            kafkaConsumer.commitSync(topicMap);
        }

    }
}

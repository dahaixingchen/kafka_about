package com.feifei.consumerCommit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
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
public class MyKafkaConsumerAsync {
    public static void main(String[] args) {
        Properties pro = new Properties();
//        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "emr-header-1:9092,emr-worker-1:9092,emr-worker-2:9092");
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "g44");
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置offset手动提交
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, false);
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest,latest

        //调用poll方法两次的时间间隔MAX_POLL_INTERVAL_MS_CONFIG，默认是300000
        pro.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,3000000);

        //调用poll方法的时候一次拿到消息的最大的值，默认是500
        pro.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,400);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(pro);

        //所有的分区都会消费到
        kafkaConsumer.subscribe(Arrays.asList("test"));
        //订阅特定的分区
//        kafkaConsumer.assign(Collections.singleton(new TopicPartition("topic", 0)));
        int count = 0;
        long i = 0;

        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            while (iterator.hasNext()) {
                i++;
                ConsumerRecord<String, String> record = iterator.next();
                System.out.println("key:" + record.key() + "---value:" + record.value()
                        + "---partition:" + record.partition() + "----offset" + record.offset());

                //实现消费到100条消息的时候就提交
                if (count % 100 == 0) {
                    Map<TopicPartition, OffsetAndMetadata> topicPartition = new HashMap<>();
                    topicPartition.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    kafkaConsumer.commitAsync(topicPartition, null);
                }
            }
            //设置offset同步提交，
            kafkaConsumer.commitAsync();


            //按时间提交
            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    System.out.println(offsets);
                    System.out.println("这是一个offset手动提交后的回调函数");
                }
            });

        }

    }
}

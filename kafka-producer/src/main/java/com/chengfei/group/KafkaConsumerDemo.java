package com.chengfei.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

// import kafka.api.OffsetRequest;

public class KafkaConsumerDemo {
    private final static String TOPIC = "ooxx";
    private final static String BOOTSTRAP_SERVERS = "node01:9092";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer"); //bula2
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        return consumer;
    }

    // 获取某个Topic的所有分区以及分区最新的Offset
    public static void getPartitionsForTopic() {
        final Consumer<Long, String> consumer = createConsumer();

        Collection<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);
        System.out.println("Get the partition info as below:");
        List<TopicPartition> tp =new ArrayList<TopicPartition>();
        partitionInfos.forEach(str -> {
            System.out.println("Partition Info:");
            System.out.println(str);

            tp.add(new TopicPartition(TOPIC,str.partition()));
            consumer.assign(tp);
            consumer.seekToEnd(tp);

            System.out.println("Partition " + str.partition() + " 's latest offset is '" + consumer.position(new TopicPartition(TOPIC, str.partition())));
        });
    }

    // 持续不断的消费数据
    public static void run() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));
        final int giveUp = 100; int noRecordsCount = 0;

        while(true){
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if(consumerRecords.count()==0){
                noRecordsCount++;
                if(noRecordsCount > giveUp) break;
                else continue;
            }

            // int i = 0;
            consumerRecords.forEach(record -> {
                // i = i + 1;
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            // System.out.println("Consumer Records " + i);
            consumer.commitAsync();
        }

        consumer.close();
        System.out.println("Kafka Consumer Exited");
    }
}
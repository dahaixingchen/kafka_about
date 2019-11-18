package com.chengfei.partition;

import com.chengfei.kafka.consumer.GetKafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Arrays;
import java.util.HashMap;

/**
 * @ClassName: ConsumerHMOffsets100
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/18 15:48
 * @Version 1.0
 **/
public class ConsumerHMOffsets100 {
    private static String bootstrap_servers = "node-1:9092";
    private static String group_id = "kafkaConsumer";
    private static int seeeion_timeout_ms = 6000;
    private static int heartbeat_interval_ms = 2000;
    private static int max_poll_interval_ms = 480000;
    private static String key_serializer_class  = "org.apache.kafka.common.serialization.StringDeserializer";
    private static String value_serialzer_class = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) {
        GetKafkaConsumer consumer = new GetKafkaConsumer(bootstrap_servers, group_id, seeeion_timeout_ms, heartbeat_interval_ms,
                max_poll_interval_ms, key_serializer_class, value_serialzer_class);
        Consumer kafkaConsumer = consumer.getKafkaConsumer();
        kafkaConsumer.subscribe(Arrays.asList("app_history"));
        HashMap<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>();
        int count = 0;
        try {
            while (true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord record: records){
                    System.out.printf("offset = %d, key = %s, value = %s%n pritition = %s%n", record.offset(), record.key(), record.value(),
                            record.partition());
                    //手动记录offers，使其不受poll方法的影响
                    offsetsMap.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1));
                    //每100条数据就提交一次offset
                    if(count % 100 == 0){
                        kafkaConsumer.commitAsync(offsetsMap,null);
                    }
                    count ++;
                }
            }
        }finally {
            kafkaConsumer.commitSync();
            kafkaConsumer.close();
        }
    }
}

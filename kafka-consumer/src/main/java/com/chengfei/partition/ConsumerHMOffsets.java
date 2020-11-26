package com.chengfei.partition;

import com.chengfei.kafka.consumer.GetKafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Arrays;

/**
 * @ClassName: ConsumerHMOffsets
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/18 15:23
 * @Version 1.0
 **/
public class ConsumerHMOffsets {
    private static String bootstrap_servers = "node-1:9092";
    private static String group_id = "kafkaConsumer";
    private static int seeeion_timeout_ms = 6000;
    private static int heartbeat_interval_ms = 2000;
    private static int max_poll_interval_ms = 480000;
    private static String key_serializer_class = "org.apache.kafka.common.serialization.StringDeserializer";
    private static String value_serialzer_class = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) {
        GetKafkaConsumer consumer = new GetKafkaConsumer(bootstrap_servers, group_id, seeeion_timeout_ms, heartbeat_interval_ms,
                max_poll_interval_ms, key_serializer_class, value_serialzer_class);
        Consumer kafkaConsumer = consumer.getKafkaConsumer();
        kafkaConsumer.subscribe(Arrays.asList("app_history")); //订阅的topic
        Duration duration = Duration.ofSeconds(1);
        System.out.println(duration);
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n pritition = %s%n", record.offset(), record.key(), record.value(),
                            record.partition());
                }
                kafkaConsumer.commitAsync();//异步提交
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            kafkaConsumer.commitSync(); //最后一步实现同步阻塞提交的方式
            kafkaConsumer.close(); //关闭资源
        }
    }
}

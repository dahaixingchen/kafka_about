package com.manwei.dataPlatform;

import com.alibaba.fastjson.JSON;
import com.manwei.dataPlatform.consumer.GetKafkaConsumer;
import com.manwei.dataPlatform.entity.ActionDataEntity;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @ClassName: Kafka2Log
 * @Author chengfei
 * @Date 2020/11/26 16:53
 * @Description: TODO
 **/
public class Kafka2Log {
    private static Logger logger = LoggerFactory.getLogger(Kafka2Log.class);
    public static void main(String[] args) {

        String bootstrap_servers = "emr-header-1:9092,emr-worker-1:9092,emr-worker-2:9092";
        String group_id = "dataPlatform";
        String topic = "cxy_action_data";
        GetKafkaConsumer kafkaConsumer = new GetKafkaConsumer(bootstrap_servers, group_id);
        Consumer consumer = kafkaConsumer.getKafkaConsumer();
        TopicPartition topicPartition1 = new TopicPartition(topic, 0);
        TopicPartition topicPartition2 = new TopicPartition(topic, 1);
        TopicPartition topicPartition3 = new TopicPartition(topic, 2);
        consumer.assign(Arrays.asList(topicPartition1, topicPartition2, topicPartition3));
        ConsumerRecords<String, String> msgList = consumer.poll(Duration.ofSeconds(1));
        System.out.println("链接成功");
        FileWriter fileWriter = null;
        while (true) {
            try {
                String day = LocalDate.now().toString();
//                File file = new File("E:\\tmp\\" + day + ".log");
                File file = new File("/root/data/log/" + day + ".log");
                if (!file.exists()) {
                    file.createNewFile();
                }
                fileWriter = new FileWriter(file, true);
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();
                while (recordIterator.hasNext()) {
                    ConsumerRecord<String, String> record = recordIterator.next();
                    String value = record.value();
                    StringBuilder data = new StringBuilder();
                    ActionDataEntity actionDataEntity = JSON.parseObject(value, ActionDataEntity.class);
                    data.append(actionDataEntity.toString()).append(String.valueOf(System.currentTimeMillis()))
                            //预留字段
                            .append(";")
                            .append(";")
                            .append(";")
                            .append(";")
                            .append(";")
                            .append(";");

                    //把数据写入文件中
                    fileWriter.write(data.append("\n").toString());
                    fileWriter.flush();
                    logger.info("数据输入文件中");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

package com.feifei.ack;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @ClassName: KafkaProducer
 * @Author chengfei
 * @Date 2020/11/28 18:37
 * @Description: TODO 普通的生产者操作
 **/
public class MyKafkaProducer {
    public static void main(String[] args) {
        Properties pro = new Properties();
//        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"emr-header-1:9092,emr-worker-1:9092,emr-worker-2:9092");
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pro.put(ProducerConfig.ACKS_CONFIG,"all");
        //生成者发送失败重试次数
        pro.put(ProducerConfig.RETRIES_CONFIG,3);
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(pro);

        for (int i = 0; i < 10; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<>("test",0,String.valueOf(i)
                    ,"xuxu你在哪里啊" + i);
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("已经成功发送");
                    if (exception != null){
                        exception.printStackTrace();
                    }
                }
            });
            kafkaProducer.send(record);
            System.out.println("消息发送成功");
        }


        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

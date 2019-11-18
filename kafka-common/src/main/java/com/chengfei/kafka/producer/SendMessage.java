package com.chengfei.kafka.producer;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.log4j.Logger;


/**
 * @ClassName: SendMessage
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/18 10:19
 * @Version 1.0
 **/
public class SendMessage {
    private static Logger logger = Logger.getLogger(SendMessage.class);

    /**
     * 发送消息到kafka中
     *
     * @Date 2019/11/15 18:13
     * @methodName sendMessage
     * @Param [tempStr]
     * @Return void
     **/
    public static void sendMessage(Producer kafkaProducer,String topic, String tempStr) {
        ProducerRecord<String, String> pr = new ProducerRecord<String, String>(topic, tempStr);
        kafkaProducer.send(pr, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (metadata == null) {
                    logger.error("send record error {}", e);
//                    System.out.println("send record error {}"+e);
                } else {
                    logger.info("offset" + metadata.offset() + "----partition" + metadata.partition());
//                    System.out.println("offset" + metadata.offset()+"----partition" + metadata.partition());
                }
            }
        });
    }

    /**
      * kafka帶事務屬性的寫入数据
      * @Date 2019/11/18 10:32
      * @methodName sendMessageOnece
      * @Param [kafkaProducer, tempStr]
      * @Return void
      **/
    public static void sendMessageOnece(Producer kafkaProducer,String topic, String tempStr) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>( topic, tempStr);
        kafkaProducer.initTransactions();

        kafkaProducer.beginTransaction();
        kafkaProducer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (metadata == null) {
                    logger.error("send record error {}", exception);
//                    System.out.println("send record error {}"+e);
                } else {
                    logger.info("offset" + metadata.offset() + "----partition" + metadata.partition());
//                    System.out.println("offset" + metadata.offset()+"----partition" + metadata.partition());
                }
            }
        });
        kafkaProducer.commitTransaction();
        kafkaProducer.abortTransaction();

    }
}

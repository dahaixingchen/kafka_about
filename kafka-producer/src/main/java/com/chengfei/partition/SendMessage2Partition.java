package com.chengfei.partition;

import com.chengfei.kafka.producer.GetKafkaProducer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @ClassName: SendMessage2Partition
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/15 14:02
 * @Version 1.0
 **/
public class SendMessage2Partition {
    private static Logger logger = LoggerFactory.getLogger(SendMessage2Partition.class);
    public static void main(String[] args) {

        Producer kafkaProducer = GetKafkaProducer.getKafkaProducer();
        //读取本地数据
        String filePath = "C:\\Users\\feifei\\Desktop\\kafka测试.txt";
        String tempStr = null;
        File file = new File(filePath);
        
        try {
            if (file.exists()){
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                while ((tempStr = bufferedReader.readLine()) != null){
                    System.out.println(tempStr);
                    sendMessage(kafkaProducer,tempStr);

                    Thread.sleep(100);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
      * 发送消息到kafka中
      * @Date 2019/11/15 18:13
      * @methodName sendMessage
      * @Param [tempStr]
      * @Return void
      **/
    private static void sendMessage(Producer kafkaProducer, String tempStr) {
        ProducerRecord<String, String> pr = new ProducerRecord<String, String>("app_history", tempStr);
        kafkaProducer.send(pr, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (metadata == null) {
                    logger.error("send record error {}", e);
                }else
                    logger.info("offset" + metadata.offset(), "----partition" + metadata.partition());

            }
        });
    }

}

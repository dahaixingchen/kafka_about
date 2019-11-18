package com.chengfei.partition;

import com.chengfei.kafka.producer.GetKafkaProducer;
import com.chengfei.kafka.producer.SendMessage;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;

import java.io.*;


/**
 * @ClassName: SendMessageOnce
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/18 10:12
 * @Version 1.0
 **/
public class SendMessageOnce {
    private static Logger logger = Logger.getLogger(SendMessage2Partition.class);

    private static String bootstrap_servers = "node-1:9092";
    private static String acks = "all";
    private static int retries = 3;
    private static Long buffer_memory = 33554432L;
    private static int bach_size = 16384;
    private static Long linger_ms = 100L;
    private static String key_serializer_class  = "org.apache.kafka.common.serialization.StringSerializer";
    private static String value_serialzer_class = "org.apache.kafka.common.serialization.StringSerializer";
    private static String partgitioner_class = "com.chengfei.kafka.producer.MyPartition";
    private static String comperssion_type = "lz4";


    public static void main(String[] args) {
        GetKafkaProducer producer = new GetKafkaProducer(bootstrap_servers, acks, retries, buffer_memory,
                bach_size, linger_ms, key_serializer_class, value_serialzer_class, partgitioner_class);
        Producer idempotenceKafkaProducer = producer.getIdempotenceKafkaProducer();

        //读取本地数据
        String filePath = "C:\\Users\\feifei\\Desktop\\无标题 4";
        String tempStr = null;
        File file = new File(filePath);

        try {
            if (file.exists()){
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                while ((tempStr = bufferedReader.readLine()) != null){
                    System.out.println(tempStr);
                    SendMessage.sendMessageOnece(idempotenceKafkaProducer,tempStr);
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
}

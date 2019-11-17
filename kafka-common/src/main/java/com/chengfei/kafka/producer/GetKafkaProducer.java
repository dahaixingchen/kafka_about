package com.chengfei.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 这是个得到生产者的类
 * @ClassName: GetKafkaProducer
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/15 16:59
 * @Version 1.0
 **/
public class GetKafkaProducer {
    /**
      * 得到kafka的生产者
     * @Description:得到kafka生产者
      * @methodName getKafkaProducer
      * @Param []
      * @Return org.apache.kafka.clients.producer.Producer
      * @Description: 得到kafka生产者
      **/
    public static Producer getKafkaProducer(){
        Properties pro = new Properties();
        //必要参数
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node-1:9092,node-2:9092,node-3:9092");
        //保证消息不丢失
        pro.put(ProducerConfig.ACKS_CONFIG, "all");//消息的确认机制,1表示对应topic的leader确认了就算成功，-1(all)表示leader和对应所有的副本,0表示无确认
        pro.put(ProducerConfig.RETRIES_CONFIG,5);//如果消费发送失败重试的次数0(默认值)
        //发送消息的吞吐(性能)设置
        pro.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);//发送一次缓存数据的最大量32M(默认值)
        pro.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);//一个批的数据量16k(默认值)
        pro.put(ProducerConfig.LINGER_MS_CONFIG,100);//结合batch_size参数,如果在此设置的时间之内,batch数据没达到也要发送出去,默认是0
        //消息序列化设置
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        pro.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.chengfei.kafka.producer.MyPartition");
        return new KafkaProducer(pro);
    }
}

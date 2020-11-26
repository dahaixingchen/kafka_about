package com.chengfei.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @ClassName: GetKafkaConsumer
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/16 21:15
 * @Version 1.0
 **/
public class GetKafkaConsumer {
    private String bootstrap_servers;
    private String group_id;
    private int seeeion_timeout_ms;
    private int heartbeat_interval_ms;
    private int max_poll_interval_ms;
    private String key_serializer_class = "org.apache.kafka.common.serialization.StringDeserializer";
    private String value_serialzer_class = "org.apache.kafka.common.serialization.StringDeserializer";

    public GetKafkaConsumer(String bootstrap_servers, String group_id, int seeeion_timeout_ms, int heartbeat_interval_ms, int max_poll_interval_ms, String key_serializer_class, String value_serialzer_class) {
        this.bootstrap_servers = bootstrap_servers;
        this.group_id = group_id;
        this.seeeion_timeout_ms = seeeion_timeout_ms;
        this.heartbeat_interval_ms = heartbeat_interval_ms;
        this.max_poll_interval_ms = max_poll_interval_ms;
        this.key_serializer_class = key_serializer_class;
        this.value_serialzer_class = value_serialzer_class;
    }

    public Consumer getKafkaConsumer() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, group_id);

        //防止rebalance发生
        pro.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, seeeion_timeout_ms);//如果再6s内没有给rebalance机制发送心跳就认为这个消费者挂了
        pro.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeat_interval_ms);//2s给Rebalance机制发送心跳的频率
        pro.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, max_poll_interval_ms); //消息处理的时间默认是5分钟

        //提交消费位移
        pro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//表示手动提交offsets
//        pro.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,5000);//如果是自动提交，这个参数的设置表示5s提交一次位移

        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //新的group id就会从头开始消费整个topic的数据
        //序列化的类
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, key_serializer_class);
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, value_serialzer_class);

        return new KafkaConsumer(pro);
    }
}

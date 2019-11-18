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
    private String bootstrap_servers;
    private String acks = "all";
    private int retries = 3;
    private Long buffer_memory = 33554432L;
    private int bach_size =  16384;
    private Long linger_ms = 100L;
    private String key_serializer_class  = "org.apache.kafka.common.serialization.StringSerializer";
    private String value_serialzer_class = "org.apache.kafka.common.serialization.StringSerializer";
    private String partgitioner_class;
    private String comperssion_type;

    public GetKafkaProducer(String bootstrap_servers) {
        this.bootstrap_servers = bootstrap_servers;
    }

    /**
      * 既不需要自定义分区，也不需要压缩
      * @Date 2019/11/17 15:24
      * @methodName GetKafkaProducer
      * @Param [bootstrap_servers, acks, retries, buffer_memory, bach_size, linger_ms, key_serializer_class, value_serialzer_class]
      * @Return
      **/
    public GetKafkaProducer(String bootstrap_servers, String acks, int retries, Long buffer_memory, int bach_size, Long linger_ms, String key_serializer_class, String value_serialzer_class) {
        this.bootstrap_servers = bootstrap_servers;
        this.acks = acks;
        this.retries = retries;
        this.buffer_memory = buffer_memory;
        this.bach_size = bach_size;
        this.linger_ms = linger_ms;
        this.key_serializer_class = key_serializer_class;
        this.value_serialzer_class = value_serialzer_class;
    }

    /**
      * 需要自定义分区，不需要压缩
      * @Date 2019/11/17 15:23
      * @methodName GetKafkaProducer
      * @Param [bootstrap_servers, acks, retries, buffer_memory, bach_size, linger_ms, key_serializer_class, value_serialzer_class, partgitioner_class]
      * @Return
      **/
    public GetKafkaProducer(String bootstrap_servers, String acks, int retries, Long buffer_memory, int bach_size, Long linger_ms, String key_serializer_class, String value_serialzer_class, String partgitioner_class) {
        this.bootstrap_servers = bootstrap_servers;
        this.acks = acks;
        this.retries = retries;
        this.buffer_memory = buffer_memory;
        this.bach_size = bach_size;
        this.linger_ms = linger_ms;
        this.key_serializer_class = key_serializer_class;
        this.value_serialzer_class = value_serialzer_class;
        this.partgitioner_class = partgitioner_class;
    }

    /**
      * 需要压缩也需要自定义分区
      * @Date 2019/11/17 15:23
      * @methodName GetKafkaProducer
      * @Param [bootstrap_servers, acks, retries, buffer_memory, bach_size, linger_ms, key_serializer_class, value_serialzer_class, partgitioner_class, comperssion_type]
      * @Return
      **/
    public GetKafkaProducer(String bootstrap_servers, String acks, int retries, Long buffer_memory, int bach_size, Long linger_ms, String key_serializer_class, String value_serialzer_class, String partgitioner_class, String comperssion_type) {
        this.bootstrap_servers = bootstrap_servers;
        this.acks = acks;
        this.retries = retries;
        this.buffer_memory = buffer_memory;
        this.bach_size = bach_size;
        this.linger_ms = linger_ms;
        this.key_serializer_class = key_serializer_class;
        this.value_serialzer_class = value_serialzer_class;
        this.partgitioner_class = partgitioner_class;
        this.comperssion_type = comperssion_type;
    }

    /**
      * 带压缩和自定义分区的生产者
      * @Date 2019/11/17 15:19
      * @methodName getCompressionKafkaProducer
      * @Param []
      * @Return org.apache.kafka.clients.producer.Producer
      **/
    public Producer getCompressionKafkaProducer(){
        Properties pro = new Properties();
        //必要参数
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);
        //保证消息不丢失
        pro.put(ProducerConfig.ACKS_CONFIG, acks);//消息的确认机制,1表示对应topic的leader确认了就算成功，-1(all)表示leader和对应所有的副本,0表示无确认
        pro.put(ProducerConfig.RETRIES_CONFIG,retries);//如果消费发送失败重试的次数0(默认值)
        //发送消息的吞吐(性能)设置
        pro.put(ProducerConfig.BUFFER_MEMORY_CONFIG,buffer_memory);//发送一次缓存数据的最大量32M(默认值)
        pro.put(ProducerConfig.BATCH_SIZE_CONFIG,bach_size);//一个批的数据量16k(默认值)
        pro.put(ProducerConfig.LINGER_MS_CONFIG,linger_ms);//结合batch_size参数,如果在此设置的时间之内,batch数据没达到也要发送出去,默认是0
        //消息序列化设置
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, key_serializer_class);
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,value_serialzer_class);
        pro.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,partgitioner_class);
        pro.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,comperssion_type);
        return new KafkaProducer(pro);
    }

    /**
      * 只是带分区的生产者
      * @Date 2019/11/17 15:44
      * @methodName getPartitionKafkaProducer
      * @Param []
      * @Return org.apache.kafka.clients.producer.Producer
      **/
    public Producer getPartitionKafkaProducer(){
        Properties pro = new Properties();
        //必要参数
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);
        //保证消息不丢失
        pro.put(ProducerConfig.ACKS_CONFIG, acks);//消息的确认机制,1表示对应topic的leader确认了就算成功，-1(all)表示leader和对应所有的副本,0表示无确认
        pro.put(ProducerConfig.RETRIES_CONFIG,retries);//如果消费发送失败重试的次数0(默认值)
        //发送消息的吞吐(性能)设置
        pro.put(ProducerConfig.BUFFER_MEMORY_CONFIG,buffer_memory);//发送一次缓存数据的最大量32M(默认值)
        pro.put(ProducerConfig.BATCH_SIZE_CONFIG,bach_size);//一个批的数据量16k(默认值)
        pro.put(ProducerConfig.LINGER_MS_CONFIG,linger_ms);//结合batch_size参数,如果在此设置的时间之内,batch数据没达到也要发送出去,默认是0
        //消息序列化设置
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, key_serializer_class);
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,value_serialzer_class);
        pro.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,partgitioner_class);
        return new KafkaProducer(pro);
    }

    /**
      * 既不带生产者也不带分区的生产者
      * @Date 2019/11/17 15:45
      * @methodName getKafkaProducer
      * @Param []
      * @Return org.apache.kafka.clients.producer.Producer
      **/
    public Producer getKafkaProducer(){
        Properties pro = new Properties();
        //必要参数
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);
        //保证消息不丢失
        pro.put(ProducerConfig.ACKS_CONFIG, acks);//消息的确认机制,1表示对应topic的leader确认了就算成功，-1(all)表示leader和对应所有的副本,0表示无确认
        pro.put(ProducerConfig.RETRIES_CONFIG,retries);//如果消费发送失败重试的次数0(默认值)
        //发送消息的吞吐(性能)设置
        pro.put(ProducerConfig.BUFFER_MEMORY_CONFIG,buffer_memory);//发送一次缓存数据的最大量32M(默认值)
        pro.put(ProducerConfig.BATCH_SIZE_CONFIG,bach_size);//一个批的数据量16k(默认值)
        pro.put(ProducerConfig.LINGER_MS_CONFIG,linger_ms);//结合batch_size参数,如果在此设置的时间之内,batch数据没达到也要发送出去,默认是0
        //消息序列化设置
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, key_serializer_class);
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,value_serialzer_class);
        return new KafkaProducer(pro);
    }

    /**
      * 具有幂等操作性的生产者
      * @Date 2019/11/18 9:28
      * @methodName getIdempotenceKafkaProducer
      * @Param []
      * @Return org.apache.kafka.clients.producer.Producer
      **/
    public  Producer getIdempotenceKafkaProducer(){
        Properties pro = new Properties();
        //必要参数
        pro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);
        //保证消息不丢失
        pro.put(ProducerConfig.ACKS_CONFIG, acks);//消息的确认机制,1表示对应topic的leader确认了就算成功，-1(all)表示leader和对应所有的副本,0表示无确认
        pro.put(ProducerConfig.RETRIES_CONFIG,retries);//如果消费发送失败重试的次数0(默认值)
        //发送消息的吞吐(性能)设置
        pro.put(ProducerConfig.BUFFER_MEMORY_CONFIG,buffer_memory);//发送一次缓存数据的最大量32M(默认值)
        pro.put(ProducerConfig.BATCH_SIZE_CONFIG,bach_size);//一个批的数据量16k(默认值)
        pro.put(ProducerConfig.LINGER_MS_CONFIG,linger_ms);//结合batch_size参数,如果在此设置的时间之内,batch数据没达到也要发送出去,默认是0
        //消息序列化设置
        pro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, key_serializer_class);
        pro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,value_serialzer_class);
        //自定义分区
        pro.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,partgitioner_class);
        //开启生产者的幂等操作
        pro.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        return new KafkaProducer(pro);
    }


}

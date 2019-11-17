package com.chengfei.kafka.producer;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: MyPartition
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/16 16:06
 * @Version 1.0
 **/
public class MyPartition implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String valueStr = (String) value;
        String[] valSpl = valueStr.split(";");
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        return Math.abs(valSpl[1].hashCode()) % partitionInfos.size();
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}

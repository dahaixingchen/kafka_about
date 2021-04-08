package com.chengfei.group;

/**
 * @Description:
 * @ClassName: KafkaConsumerDTO
 * @Author chengfei
 * @DateTime 2021/4/8 9:23
 **/
public class KafkaConsumerDTO {
    private String groupId;
    private String topic;
    private int partition;
    private long currentOffset;


    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    @Override
    public String toString() {
        return "KafkaConsumerDTO{" +
                "groupId='" + groupId + '\'' +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", currentOffset=" + currentOffset +
                '}';
    }
}

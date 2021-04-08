package com.chengfei.group;

import com.sun.scenario.effect.Offset;
import org.apache.kafka.clients.admin.AdminClient;
import kafka.common.TopicAndPartition;
import kafka.coordinator.group.GroupOverview;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaListenerServiceImpl {

    /**
     * 获取服务器上所有的groupId
     *
     * @return
     */
    public Set<String> getAllGroupId() {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");

        AdminClient client = AdminClient.create(map);

        Set<String> groupIds = new HashSet<String>();
        try {
            // 1.查询到所有的 groupId对象
            ListConsumerGroupsResult listConsumerGroupsResult = client.listConsumerGroups();
            KafkaFuture<Collection<ConsumerGroupListing>> allGroupId = listConsumerGroupsResult.all();
            allGroupId.get().forEach(group -> {
                groupIds.add(group.groupId());
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
        return groupIds;
    }

    /**
     * 获取具体消费情况
     *
     * @return
     */
    public List<KafkaConsumerDTO> getConsumerDetails( Set<String> groupIds) throws ExecutionException, InterruptedException {
        List<KafkaConsumerDTO> kafkaConsumerDtoS = new ArrayList<>();

        ArrayList<String> topics = new ArrayList<>();
        topics.add("bala");


        Map<String, Object> map = new HashMap<String, Object>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");

        Map<String, TopicDescription> descriptionMap = AdminClient.create(map).describeTopics(topics).all().get();
        descriptionMap.forEach((k,v)->{
            System.out.println(k);
//            System.out.println(v.);
        });

        groupIds.forEach(groupId -> {
            AdminClient client = AdminClient.create(map);
            try {
                // 获取当前groupId消费情况
                Map<TopicPartition, OffsetAndMetadata> offsets = client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();


                for (TopicPartition topicPartition : offsets.keySet()) {



                    String topic = topicPartition.topic();
                    int partition = topicPartition.partition();
                    OffsetAndMetadata offset = offsets.get(topicPartition);
                    long currentOffset = offset.offset();

                    // 数据组装
                    KafkaConsumerDTO kafkaConsumerDTO = new KafkaConsumerDTO();
                    kafkaConsumerDTO.setGroupId(groupId);
                    kafkaConsumerDTO.setTopic(topic);
                    kafkaConsumerDTO.setPartition(partition);
                    kafkaConsumerDTO.setCurrentOffset(currentOffset);

                    String metadata = offset.metadata();
                    System.out.println(kafkaConsumerDTO);
                    System.out.println(metadata);


//                    String clientName = "Client_" + topic + "_" + partition;
//                    SimpleConsumer consumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, clientName);
//                    long logEndOffset = getLastOffset(consumer, topic, partition, OffsetRequest.LatestTime(), clientName);
//
//                    // 节点不是该分区的leader获取不到最后提交的偏移量logEndOffset
//                    if (logEndOffset != 0){
//                        kafkaConsumerDTO.setLogEndOffset(logEndOffset);
//                        kafkaConsumerDTO.setLag(logEndOffset - kafkaConsumerDTO.getCurrentOffset());
//                        kafkaConsumerDTO.setHost(broker);
//                        kafkaConsumerDtoS.add(kafkaConsumerDTO);
//                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } finally {
                client.close();
            }
        });

        return kafkaConsumerDtoS;
    }


//    /**
//     * 获取该消费者组每个分区最后提交的偏移量
//     *
//     * @param consumer   消费者组对象
//     * @param topic      主题
//     * @param partition  分区
//     * @param whichTime  最晚时间
//     * @param clientName 客户端名称
//     * @return 偏移量
//     */
//    private static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
//        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
//        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
//        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
//        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
//        OffsetResponse response = consumer.getOffsetsBefore(request);
//        if (response.hasError()) {
////            log.warn("错误代码：" + response.errorCode(topic, partition));
//            return 0;
//        }
//        long[] offsets = response.offsets(topic, partition);
//        return offsets[0];
//    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaListenerServiceImpl kafkaListenerService = new KafkaListenerServiceImpl();
        Set<String> allGroupId = kafkaListenerService.getAllGroupId();
        List<KafkaConsumerDTO> consumerDetails = kafkaListenerService.getConsumerDetails(allGroupId);
        allGroupId.forEach(e -> System.out.println(e));
//        allGroupId.forEach(group -> {
//            System.out.println(group);
//            List<KafkaConsumerDTO> consumerDetails = kafkaListenerService.getConsumerDetails(group);
//            consumerDetails.forEach(offset -> {
//                System.out.println(offset);
//            });
//        });
    }
}

package com.feifei.queryStart;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName: KafkaAdminClient
 * @Author chengfei
 * @Date 2020/11/28 18:49
 * @Description: TODO kafaka admin的操作，可以对topic进行增删改查，等DDL操作
 **/
public class MyKafkaAdminClient {
    static Logger logger = LoggerFactory.getLogger(MyKafkaAdminClient.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Map<String, Object> pro = new HashMap<>();
        pro.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        AdminClient adminClient = KafkaAdminClient.create(pro);

        adminClient.createTopics(Arrays.asList(new NewTopic("topic",3, (short) 2)));


        logger.info("创建的topic成功");
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topics = listTopicsResult.names().get();
        for (String topic : topics) {
            System.out.println("topic的名称" + topic);
        }
        adminClient.close();
    }
}

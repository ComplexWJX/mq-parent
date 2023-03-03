package com.asiainfo.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * [一句话描述类功能]
 *
 * @author rukawa
 * Created on 2023/02/08 17:53 by rukawa
 */
public class AdminClientTool {

    public final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void createTopic(String topicName, int partition, short replicationFactors) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        // 设置发送主题、分区

        NewTopic newTopic = new NewTopic(topicName, partition, replicationFactors);

        AdminClient adminClient = AdminClient.create(properties);
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(String.format("create topic %s failed.", topicName));
        }

        System.out.println(String.format("create topic %s success.", topicName));
    }

    public static void deleteTopic(String topicName) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);

        AdminClient adminClient = AdminClient.create(properties);


        /*
        * 没有在server.properties配置：delete.topic.enable=true，只是标记为 marked for deletion
        * 彻底删除：
        *   1.delete.topic.enable=true，重启删除；
        *   2.zkCli登录，deleteall /brokers/topics/{topic name}；
        *   3.删除kafka的logs目录下topic相关文件
        * */
        DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));

        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(String.format("delete topic %s failed.", topicName));
        }

        System.out.println(String.format("delete topic %s success.", topicName));
    }

    public static Collection<TopicListing> listTopics() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);

        AdminClient adminClient = AdminClient.create(properties);

        ListTopicsResult listTopicsResult = adminClient.listTopics();

        try {
            return listTopicsResult.listings().get();
        } catch (InterruptedException | ExecutionException e) {
            return Collections.emptyList();
        }
    }

    public static Map<String, TopicDescription> describeTopics(String topicName) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);

        AdminClient adminClient = AdminClient.create(properties);

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));

        try {
            return describeTopicsResult.allTopicNames().get();
        } catch (InterruptedException | ExecutionException e) {
            return Collections.emptyMap();
        }
    }
}

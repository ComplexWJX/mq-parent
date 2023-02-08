package com.asiainfo.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Test;

import java.util.Properties;

/**
 * [一句话描述类功能]
 *
 * @author rukawa
 * Created on 2023/02/08 17:53 by rukawa
 */
public class AdminClientTool {

    @Test
    public void testCreateTopic() {
        createTopic("", "");
    }


    public static void createTopic(String topicName, String partition) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("topic", topicName);
        // 设置发送主题、分区
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("num.partitions", partition);

        AdminClient.create(properties);
    }
}

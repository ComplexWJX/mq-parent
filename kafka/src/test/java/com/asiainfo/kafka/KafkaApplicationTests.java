package com.asiainfo.kafka;

import com.asiainfo.kafka.admin.AdminClientTool;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Collection;
import java.util.Map;

@SpringBootTest
class KafkaApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    public void testCreateTopic() {
        AdminClientTool.createTopic("order", 3, (short) 3);
    }

    @Test
    public void testDeleteTopic() {
        AdminClientTool.deleteTopic("order");
    }

    @Test
    public void testListTopic() {
        Collection<TopicListing> topicListings = AdminClientTool.listTopics();
        System.out.println(topicListings);
    }

    @Test
    public void testDescribeTopics() {
        Map<String, TopicDescription> descriptionMap = AdminClientTool.describeTopics("order");
        System.out.println(descriptionMap);
    }

}

package com.asiainfo.kafka;

import com.asiainfo.kafka.admin.AdminClientTool;
import com.asiainfo.kafka.consumer.KafkaOriginalConsumer;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@SpringBootTest
class KafkaApplicationTests {

    public static void main(String[] args) {
//        KafkaApplicationTests tests = new KafkaApplicationTests();
//        tests.testDescribeTopics();
        new KafkaOriginalConsumer().start();
//        new KafkaOriginalConsumer().start();
    }

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

    @Test
    public void testModifyOffsetForReConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-master:9092,kafka-slave1:9093,kafka-slave2:9094");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //使用不同的组名
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup2");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
        String topic = "order";
        //指定从offset==0开始消费
        int offset = 0;
        int partition = 1;
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        /*
         * 1.standalone consumer:指consumer.assign()而非consumer.subscribe()的消费者
         *      consumer group:指consumer.subscribe的消费者
         *      consumer.assign()和consumer.subscribe()不能同时使用
         * 2. 当用户系统中同时出现了standalone consumer和consumer group，并且它们的group id相同时，
         * 此时standalone consumer手动提交位移时就会立刻抛出CommitFailedException。所以不要让
         * standalone consumer和consumer.subscribe()的groupId一样，这里指定的是testgroup2
         */

        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(new TopicPartition(topic, partition), offset);

        // consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(1, ChronoUnit.MILLIS));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
            consumer.commitSync();
        }

    }

}

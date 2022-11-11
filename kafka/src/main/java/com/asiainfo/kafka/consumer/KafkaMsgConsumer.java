package com.asiainfo.kafka.consumer;

import com.asiainfo.kafka.producer.AsyncProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * [一句话描述类功能]
 *
 * @author rukawa
 * Created on 2022/11/04 11:34 by rukawa
 */
public class KafkaMsgConsumer {

    private final static Logger logger = LoggerFactory.getLogger(AsyncProducer.class);

    KafkaConsumer<String, Object> kafkaConsumer;

    @PostConstruct
    public void init() {

        subscribe();

        listenMsg();
    }

    public KafkaMsgConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // 消费者，同一个消费组的消费者消费同一个主题，todo 如何在其他消费者挂掉的情况下接手offset？
        properties.setProperty("group.id", "MyGroup");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //可以取值为latest（从最新的消息开始消费）或者earliest（从最老的消息开始消费）
        properties.put("auto.offset.reset", "earliest");
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        this.kafkaConsumer = new KafkaConsumer<>(properties);
    }

    public void subscribe() {
        // 单独订阅主题
        kafkaConsumer.subscribe(Collections.singletonList("order"));

        // 订阅分区
//        Collection<TopicPartition> topicPartitions = new ArrayList<>();
//        TopicPartition partition = new TopicPartition("order", 0);
//        topicPartitions.add(partition);
//        kafkaConsumer.assign(topicPartitions);
        //kafkaConsumer.seekToEnd(topicPartitions);
    }

    public void listenMsg() {
        new Thread(() -> {
            while (true) {

                ConsumerRecords<String, Object> consumerRecords = kafkaConsumer.poll(Duration.of(500, ChronoUnit.MILLIS));

                if (!consumerRecords.isEmpty()) {
                    for (ConsumerRecord<String, Object> record : consumerRecords) {
                        // todo
                        logger.info("offset:{}",  record.offset());
                        logger.info("value:{}", record.value());
                    }
                    // auto.commit.offset设置false
                    //kafkaConsumer.commitAsync();
                }
            }
        }).start();
    }
}

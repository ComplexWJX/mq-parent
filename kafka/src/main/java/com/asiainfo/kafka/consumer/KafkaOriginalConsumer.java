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
import java.util.Properties;

/**
 * [消费者配置：方式一，使用原生Kafka api]
 *
 * @author rukawa
 * Created on 2022/11/04 11:34 by rukawa
 */
public class KafkaOriginalConsumer {

    private final static Logger logger = LoggerFactory.getLogger(AsyncProducer.class);

    KafkaConsumer<String, Object> kafkaConsumer;

    @PostConstruct
    public void init() {

        //subscribe();

        //listenMsg();
    }

    public KafkaOriginalConsumer() {
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
        // 只订阅主题
        //kafkaConsumer.subscribe(Collections.singletonList("order"));

        // 订阅主题和分区
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        TopicPartition partition = new TopicPartition("order", 0);
        topicPartitions.add(partition);
        kafkaConsumer.assign(topicPartitions);
        kafkaConsumer.seekToEnd(topicPartitions);
    }

    public void listenMsg() {
        new Thread(() -> {
            try {
                while (true) {
                    long position = kafkaConsumer.position(new TopicPartition("order", 0));
                    logger.info("position now is : {}", position);
                    ConsumerRecords<String, Object> consumerRecords = kafkaConsumer.poll(Duration.of(500, ChronoUnit.MILLIS));

                    if (!consumerRecords.isEmpty()) {
                        for (ConsumerRecord<String, Object> record : consumerRecords) {
                            // todo
                            logger.info("msg offset:{}",  record.offset());
                            logger.info("receive msg, the value is :{}", record.value());
                        }
                        // auto.commit.offset设置false
                        kafkaConsumer.commitAsync();
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        logger.error("thread is interrupted..", e);
                    }
                }
            }
            catch (Exception e) {
                logger.error("occured error", e);
            } finally {
                kafkaConsumer.close(Duration.ofMillis(500));
            }
        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumer.wakeup();
            logger.debug("consumer is closed gracefully.");
        }));
    }
}

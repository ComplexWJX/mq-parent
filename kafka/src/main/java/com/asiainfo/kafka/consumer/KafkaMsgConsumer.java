package com.asiainfo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

/**
 * [一句话描述类功能]
 *
 * @author rukawa
 * Created on 2022/11/04 11:34 by rukawa
 */
public class KafkaMsgConsumer {
    KafkaConsumer<String, Object> kafkaConsumer;

    public KafkaMsgConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker1:9092");
        // 消费者，同一个消费组的消费者消费同一个主题，todo 如何在其他消费者挂掉的情况下接手offset？
        properties.setProperty("group.id", "MyGroup");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.kafkaConsumer = new KafkaConsumer<>(properties);
    }

    public void subscribe(List<String> topics) {
        kafkaConsumer.subscribe(topics);
    }

    public void listenMsg() {
        while (!Thread.interrupted()) {
            ConsumerRecords<String, Object> consumerRecords = kafkaConsumer.poll(Duration.of(3000, ChronoUnit.MILLIS));
            if (!consumerRecords.isEmpty()) {
                for (ConsumerRecord<String, Object> record : consumerRecords) {
                    // todo
                    System.out.println("offset:" + record.offset());
                    System.out.println("value:" + record.value());
                }
                // auto.commit.offset设置false
                kafkaConsumer.commitAsync();
            }
        }
    }
}

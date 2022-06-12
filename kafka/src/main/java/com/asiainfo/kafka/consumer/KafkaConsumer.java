package com.asiainfo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private final static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = {"order"})
    public void listenMessageOnTopic(ConsumerRecord<?, ?> consumerRecord) {
        Object value = consumerRecord.value();
        logger.info("receive message from topic {}, value is {}", consumerRecord.topic(), value.toString());
    }

    @KafkaListener(topicPartitions = {@TopicPartition(topic = "order", partitionOffsets = {@PartitionOffset(partition = "1", initialOffset = "0", relativeToCurrent = "true")})})
    public void listenMessageOnPartition(ConsumerRecord<?, ?> consumerRecord) {
        Object value = consumerRecord.value();
        logger.info("receive message from partition {} - topic {}, value is {}", consumerRecord.partition(), consumerRecord.topic(), value.toString());
    }
}

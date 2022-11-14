package com.asiainfo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * [消费者配置：方式二，spring @KafkaListener]
 * @author rukawa
 * Created on 2022/11/14 13:26 by rukawa
 */
@Component
public class KafkaListenerConsumer {
    private final static Logger logger = LoggerFactory.getLogger(KafkaListenerConsumer.class);

    /**
     * enable-auto-commit设置true，自动提交
     * @param consumerRecord
     */
    //@KafkaListener(topics = {"order"})
    public void listenMessageOnTopic(ConsumerRecord<?, ?> consumerRecord) {
        Object value = consumerRecord.value();
        logger.info("receive message from topic {}, value is {}", consumerRecord.topic(), value.toString());
    }

    /**
     * enable-auto-commit设置false，手动提交
     * 需要设置 listener.ack-mode 配置
     * @param consumerRecord
     */
    //@KafkaListener(topics = {"order"})
    public void listenMessageOnTopic(ConsumerRecord<?, ?> consumerRecord, Acknowledgment ack) {
        Object value = consumerRecord.value();
        logger.info("receive message from topic {}, value is {}", consumerRecord.topic(), value.toString());
        ack.acknowledge();
    }

    //@KafkaListener(topicPartitions = {@TopicPartition(topic = "order", partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0", relativeToCurrent = "true")})})
    public void listenMessageOnPartition(ConsumerRecord<?, ?> consumerRecord) {
        Object value = consumerRecord.value();
        logger.info("receive message from partition {} - topic {}, value is {}", consumerRecord.partition(), consumerRecord.topic(), value.toString());
    }
}

package com.asiainfo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.util.Properties;


/**
 * [消费者配置：方式三，spring MessageListenerContainer]
 *
 * @author rukawa
 * Created on 2022/11/14 11:33 by rukawa
 */
//@Configuration
public class MessageListenerConfiguration {
    private final static Logger logger = LoggerFactory.getLogger(KafkaListenerConsumer.class);

    @Autowired
    private ConsumerFactory<String, Object> consumerFactory;

    @Bean
    public KafkaMessageListenerContainer<String, Object> messageListenerContainer() {
        ContainerProperties containerProperties = new ContainerProperties("order");
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProperties.setMessageListener(new MessageListener<String, Object>() {
            @Override
            public void onMessage(ConsumerRecord<String, Object> consumerRecord) {
                logger.info("receive message from topic {}, value is {}", consumerRecord.topic(), consumerRecord.value().toString());
            }
        });
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "container.group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory(properties, new StringDeserializer(), new StringDeserializer());
        return consumerFactory;
    }
}

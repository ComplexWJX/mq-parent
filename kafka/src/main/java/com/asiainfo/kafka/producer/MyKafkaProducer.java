package com.asiainfo.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * [一句话描述类功能]
 *
 * @author rukawa
 * Created on 2023/03/10 14:04 by rukawa
 */
@Slf4j
public class MyKafkaProducer {

    public final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    private final Properties properties = new Properties();

    public MyKafkaProducer() {

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
    }

    public String sendAsync(String topic, int partition) {

        KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, partition);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    log.info("send successfully. metadata is: {}", metadata);
                } else {
                    log.info("send failed, exception is : {}", exception.getMessage());
                }
            }
        });

        return "send msg to topic: " + topic + " successfully";
    }

    public String sendSync(String topic, int partition) {

        KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, partition, "order-mxbc", "order2023031201");
        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);

        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.info("send failed, exception is : {}", e.getMessage());
        }

        return "send msg to topic: " + topic + " successfully";
    }
}

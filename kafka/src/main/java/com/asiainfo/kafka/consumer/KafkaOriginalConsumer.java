package com.asiainfo.kafka.consumer;

import com.asiainfo.kafka.producer.AsyncProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * [消费者配置：方式一，使用原生Kafka api]
 *
 * @author rukawa
 * Created on 2022/11/04 11:34 by rukawa
 */
public class KafkaOriginalConsumer {

    private final static Logger logger = LoggerFactory.getLogger(AsyncProducer.class);

    public final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    KafkaConsumer<String, Object> kafkaConsumer;

    public KafkaOriginalConsumer() {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        /*
        * 其他消费者挂掉的情况时，发生再均衡，分区重新分配。consumer客户端需要读取分区最新偏移量，
        * 客户端正在处理的offset与分区最后提交的offset可能不一致。
        * 客户端正在处理的偏移量大于分区最后提交的offset，则出现消息丢失，反之则出现重复消费
        * */
        // 消费组，topic一个partition只能被消费组中一个消费者消费
        properties.setProperty("group.id", "MyGroup");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //可以取值为latest（从最新的消息开始消费）或者earliest（从最老的消息开始消费）
        properties.put("auto.offset.reset", "earliest");

        // poll最大条数 说明：与poll传入参数Duration有关联，MAX_POLL_RECORDS_CONFIG和Duration哪个条件先满足，poll就返回
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);

        // poll间隔最大时长
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 2 * 1000);

        // poll期间，心跳发送间隔最大时长
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);

        // 会话保持最大时长
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 3 * 1000);

        this.kafkaConsumer = new KafkaConsumer<>(properties);

        if (null == properties.get(ConsumerConfig.GROUP_ID_CONFIG)) {
            subscribe();
        } else {
            assign();
        }

    }

    public void start() {
        pollMsg();

        shutdownGracefully();
    }

    public void subscribe() {
        // 订阅主题，有新的分区加入，会通知到consumer
        kafkaConsumer.subscribe(Collections.singletonList("order"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 发生在分区再均衡开始之前，消费者停止读取消息后。持久化分区和偏移量信息
                // storeTopicPartitionsInDB
                logger.info("execute before partitionsRevoked.");
                logger.info(partitions.toString());
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 发生在分区再均衡完成之后，消费者开始读取消息前。从持久层恢复分区和偏移量信息
                // getTopicPartitionsFromDB
                logger.info("execute after partitionsAssigned.");
                logger.info(partitions.toString());
            }
        });
    }

    public void assign() {
        // 自我分配主题和分区，有新的分区加入，consumer不会读取到新分区的消息，除非调用partitionsFor读取新的分区信息
        Collection<TopicPartition> topicPartitions = new ArrayList<>();
        TopicPartition partition = new TopicPartition("order", 0);
        topicPartitions.add(partition);
        // 指定偏移量
        //kafkaConsumer.seekToEnd(topicPartitions);
        kafkaConsumer.assign(topicPartitions);
    }

    public void pollMsg() {

        Thread t = new Thread(() -> {
            try {
//                TopicPartition partition = new TopicPartition("order", 0);
                while (!Thread.interrupted()) {
//                    long position = kafkaConsumer.position(partition);
//                    logger.info("position now is : {}", position);
                    ConsumerRecords<String, Object> consumerRecords = kafkaConsumer.poll(Duration.of(500, ChronoUnit.MILLIS));

                    if (!consumerRecords.isEmpty()) {
                        for (ConsumerRecord<String, Object> record : consumerRecords) {
                            // todo
                            logger.info("msg offset:{}", record.offset());
                            logger.info("receive msg, the value is :{}", record.value());
                        }
                        // auto.commit.offset设置false
                        kafkaConsumer.commitAsync();
                    }
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        Thread.currentThread().interrupt();
//                        logger.warn("consumer thread is interrupted..", e);
//                    }
                }
            } catch (Exception e) {
                logger.error("some error occurred", e);
            } finally {
                kafkaConsumer.close(Duration.ofMillis(500));
            }
        });

        t.start();

//        try {
//            TimeUnit.SECONDS.sleep(3);
//            t.interrupt();
//        } catch (InterruptedException e) {
//            logger.error("some error occurred", e);
//        }
    }

    public void shutdownGracefully() {
        // jvm退出之前执行
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumer.wakeup();
            logger.warn("consumer is closed gracefully.");
        }));

    }
}

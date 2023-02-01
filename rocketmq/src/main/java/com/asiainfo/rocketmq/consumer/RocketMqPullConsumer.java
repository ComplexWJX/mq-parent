package com.asiainfo.rocketmq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * [一句话描述类功能]
 *
 * @author rukawa
 * Created on 2023/01/13 9:54 by rukawa
 */
@Slf4j
public class RocketMqPullConsumer {
//    DefaultMQPullConsumer mqPushConsumer; // 过时,需要自己管理offset

    DefaultLitePullConsumer mqPullConsumer;

    public RocketMqPullConsumer() {
        mqPullConsumer = new DefaultLitePullConsumer();
        mqPullConsumer.setNamesrvAddr("localhost:9876");
        mqPullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqPullConsumer.setConsumerGroup("my-consumer-group");
        try {
            mqPullConsumer.subscribe("bdReqtest", "");
            mqPullConsumer.start();
        } catch (MQClientException e)
        {
            e.printStackTrace();
        }
    }

    public String pollMessage(String topic) {
        StringBuilder msgSb = new StringBuilder();
        try {
            Collection<MessageQueue> messageQueues = mqPullConsumer.fetchMessageQueues(topic);
            for (MessageQueue messageQueue : messageQueues) {
                log.info("fetch messageQueue from broker : {}, queue id is: {}", messageQueue.getBrokerName(), messageQueue.getQueueId());

                mqPullConsumer.seek(messageQueue, 1);
                List<MessageExt> messageExtList = mqPullConsumer.poll(3000);
                if (!messageExtList.isEmpty()) {
                    messageExtList = mqPullConsumer.poll(3000);
                    for (MessageExt messageExt : messageExtList) {
                        String topicName = messageExt.getTopic();
                        String msgContent = new String(messageExt.getBody());
                        log.info("fetch msg from topic : {}, msg is: {}", topicName, msgContent);
                        msgSb.append(msgContent).append(",");
                    }
                    mqPullConsumer.commitSync();
                }
            }

            //mqPullConsumer.shutdown();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        return msgSb.toString();
    }

    public static void main(String[] args) {
        String result = new RocketMqPullConsumer().pollMessage("test_topic_async");
        System.out.println(result);
    }
}

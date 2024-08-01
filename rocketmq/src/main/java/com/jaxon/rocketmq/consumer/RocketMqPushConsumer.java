package com.jaxon.rocketmq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * [一句话描述类功能]
 *
 * @author rukawa
 * Created on 2023/01/13 9:54 by rukawa
 */
@Slf4j
public class RocketMqPushConsumer {
    DefaultMQPushConsumer mqPushConsumer;

    public RocketMqPushConsumer() throws MQClientException {
        mqPushConsumer = new DefaultMQPushConsumer();
        mqPushConsumer.setNamesrvAddr("localhost:9876");
        mqPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        mqPushConsumer.setConsumerGroup("my-consumer-group");
        mqPushConsumer.subscribe("test_topic", "");
        mqPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                String result = onReceive(msgs, context);
                System.out.println(result);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        mqPushConsumer.start();
    }

    public String onReceive(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        StringBuilder msgSb = new StringBuilder();
        if (!msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                msgSb.append(new String(msg.getBody()));
            }
        }
        return msgSb.toString();
    }

    public static void main(String[] args) {
        try {
            new RocketMqPushConsumer();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}

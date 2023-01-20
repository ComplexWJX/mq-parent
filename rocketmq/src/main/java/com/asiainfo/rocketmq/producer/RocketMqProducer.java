package com.asiainfo.rocketmq.producer;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * [RocketMq生产者api封装]
 * windows下启动broker，[start mqbroker或者.\mqbroker方式启动]必须必须指定 -n参数，即namerserver地址，
 * 否则broker无法注册到nameserver
 *
 * @author rukawa
 * Created on 2023/01/13 9:26 by rukawa
 */
@Slf4j
public class RocketMqProducer {

    DefaultMQProducer defaultMQProducer = new DefaultMQProducer();

    public RocketMqProducer() throws MQClientException {
        defaultMQProducer.setNamesrvAddr("10.62.10.162:9876");
        defaultMQProducer.setRetryTimesWhenSendFailed(1);
        defaultMQProducer.setProducerGroup("myGroup");
        defaultMQProducer.setSendMsgTimeout(15000);
        defaultMQProducer.setInstanceName("mq-producer1");
        defaultMQProducer.start();
    }

    public String sendSync() {
        SendResult sendResult = null;
        Message message = new Message();
        message.setTopic("test_topic_1");
        for (int i = 0; i < 5; i++) {
            message.setBody(("order" + System.currentTimeMillis()).getBytes());

            try {
                sendResult = defaultMQProducer.send(message);
            } catch (MQClientException | RemotingException | InterruptedException | MQBrokerException e) {
                log.error("send message to rocket server failed", e);
            }
        }
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        defaultMQProducer.shutdown();
        return sendResult != null ? sendResult.getSendStatus().name() : "failed";
    }


    public String sendAsync() {
        Message message = new Message();
        message.setTopic("test_topic_async");
        message.setBody(("order_" + System.currentTimeMillis()).getBytes());

        try {
            // timeout单位ms
            defaultMQProducer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("send result: {}", JSONObject.toJSONString(sendResult));
                }

                @Override
                public void onException(Throwable throwable) {
                    throwable.printStackTrace();
                    log.info("send failed, error msg is : {}", throwable.getMessage());
                }
            }, 3000L);
        } catch (MQClientException | RemotingException | InterruptedException e) {
            log.error("send message to rocket server failed", e);
            return "failed";
        }

        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        defaultMQProducer.shutdown();
        return "success";
    }

    public static void main(String[] args) {
        try {
//            String result = new RocketMqProducer().sendSync();
            String result = new RocketMqProducer().sendAsync();
            System.out.println(result);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}

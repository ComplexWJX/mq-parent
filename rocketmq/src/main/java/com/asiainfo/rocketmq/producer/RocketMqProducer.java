package com.asiainfo.rocketmq.producer;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * [一句话描述类功能]
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
        defaultMQProducer.start();
    }

    public String sendSync() {
        Message message = new Message();
        message.setTopic("test_topic");
        //message.setInstanceId("");
        for (int i = 0; i < 10; i++) {
            message.setBody(("order" + i).getBytes());
            try {
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
                }, 10L);
            } catch (MQClientException | RemotingException | InterruptedException e) {
                log.error("send message to rocket server failed", e);
            }
        }
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        defaultMQProducer.shutdown();
        return "success";
    }

    public static void main(String[] args) {
        try {
            String result = new RocketMqProducer().sendSync();
            System.out.println(result);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}

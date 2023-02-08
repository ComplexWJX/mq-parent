package com.asiainfo.kafka.producer;

import com.alibaba.fastjson.JSON;
import com.asiainfo.kafka.bean.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 测试同步发送与监听
 * @author rukawa
 * Created on 2022/11/10 19:30 by rukawa
 */
@RestController
public class AsyncProducer {

    private final static Logger logger = LoggerFactory.getLogger(AsyncProducer.class);

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 同步发送
     * @param msg 消息
     * @throws Exception
     */
    @GetMapping("/kafka/sync/{msg}")
    @ResponseBody
    public String syncSend(@PathVariable("msg") String msg) throws Exception {
        Message message = new Message();
        message.setMessage(msg);
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send("order", 0,"myKey",  JSON.toJSONString(message));
        //注意，可以设置等待时间，超出后，不再等候结果
        SendResult<String, Object> result = future.get(3, TimeUnit.SECONDS);
        logger.info("send result:{}", result.getProducerRecord().value());
        return "success," + result.getProducerRecord().value();
    }

}

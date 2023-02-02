package com.asiainfo.rocketmq.access;

import com.asiainfo.rocketmq.consumer.RocketMqPullConsumer;
import com.asiainfo.rocketmq.producer.RocketMqProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 测试同步发送与监听
 * @author rukawa
 * Created on 2022/11/10 19:30 by rukawa
 */
@RestController
public class MsgAccess {

    private final static Logger logger = LoggerFactory.getLogger(MsgAccess.class);

    private final RocketMqProducer rocketMqProducer =  new RocketMqProducer();

    private final RocketMqPullConsumer mqPullConsumer =  new RocketMqPullConsumer();

    /**
     * 同步发送
     * @param topic 消息主题
     * @throws Exception
     */
    @PostMapping("/rocket/sendMsg")
    @ResponseBody
    public String syncSend(@RequestParam("topic") String topic, @RequestBody String msg) throws Exception {
        //String result = rocketMqProducer.sendSync(topic, msg);
        String result = rocketMqProducer.sendToSpecifiedQueue(topic, msg);
        return result;
    }

    /**
     * 拉取消息
     * @param topic 消息主题
     * @throws Exception
     */
    @GetMapping(value = "/rocket/pullMsg", produces = "application/json;charset=utf-8")
    @ResponseBody
    public String pullMsg(@RequestParam("topic") String topic) throws Exception {
        String result = mqPullConsumer.pollMessage(topic);
        return result;
    }

    public void createTopic(@RequestParam String topicName, @RequestParam String partition) {
    }

}

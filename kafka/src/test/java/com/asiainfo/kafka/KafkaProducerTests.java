package com.asiainfo.kafka;

import com.asiainfo.kafka.producer.MyKafkaProducer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaProducerTests {

    @Test
    public void testSendMsg() {
        MyKafkaProducer producer = new MyKafkaProducer();
        producer.sendSync("order", 0);
    }

}

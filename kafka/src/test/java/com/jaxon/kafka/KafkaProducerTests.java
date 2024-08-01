package com.jaxon.kafka;

import com.jaxon.kafka.producer.MyKafkaProducer;
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

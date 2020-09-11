package com.asiainfo.aif.jms.activeMQ.single;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

@Slf4j
public class MyActiveMQProducer implements Runnable{

    private final static String DEFAULT_USER = ActiveMQConnection.DEFAULT_USER;
    private final static String DEFAULT_PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    private final static String DEFAULT_BROKER_URL = ActiveMQConnection.DEFAULT_BROKER_URL;

    @Override
    public void run() {
        try {
            /**
             * ActiveMQ的session负责创建队列queue，生产者producer，消息message
             */

            // Create a ConnectionFactory
            ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(DEFAULT_USER,DEFAULT_PASSWORD,DEFAULT_BROKER_URL);
            // Create a Connection
            Connection connection = activeMQConnectionFactory.createConnection();
            connection.start();
            // Create a Session
            Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue("TEST.QUEUE");

            // Create a MessageProducer from the Session to the Topic or Queue
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // Create a messages
            String msg = "hello,boy.This is a helloWorld of activeMQ!";
            TextMessage textMessage = session.createTextMessage(msg);

            // Tell the producer to send the message
            log.info("textMessage:"+textMessage);
            producer.send(textMessage);

            // release connection
            session.close();
            connection.close();

        } catch (Exception e) {
            log.error("create message failed",e);
        }
    }
}

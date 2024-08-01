package com.jaxon.activeMq.single;

import com.jaxon.activeMq.message.MessageBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

@Slf4j
public class MyActiveMQConsumer implements Runnable, ExceptionListener {

    private final static String DEFAULT_USER = ActiveMQConnection.DEFAULT_USER;
    private final static String DEFAULT_PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    private final static String DEFAULT_BROKER_URL = ActiveMQConnection.DEFAULT_BROKER_URL;

    Connection connection;

    Session session;

    MessageConsumer consumer;

    @Override
    public void run() {
        try {
            //create connection
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(DEFAULT_USER,DEFAULT_PASSWORD,DEFAULT_BROKER_URL);
            connection = connectionFactory.createConnection();
            connection.start();
            connection.setExceptionListener(this);

            //create session
            session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);

            //create queue
//            Topic topic = session.createTopic("TEST.QUEUE");
            Destination topic = session.createQueue("TEST");

            //create consumer
            consumer = session.createConsumer(topic);

            //consume the message
            while (!Thread.currentThread().isInterrupted()){
                Message message = consumer.receive(3000);
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    System.out.println("Received: " + text);
                }else if(message instanceof MessageBean){
                    MessageBean messageBean = (MessageBean) message;
                    log.info(messageBean.toString());
                }
                log.info("receive message {}",message);
            }
        } catch (Exception e) {
            log.error("consume msg error",e);
        }finally {
            if(null !=connection){
                try {
                    //release
                    consumer.close();
                    session.close();
                    connection.close();
                } catch (JMSException e) {
                    log.error("shutdown failed",e);
                }
            }
        }
    }

    @Override
    public void onException(JMSException e) {
        log.error("JMS Exception occured.  Shutting down client.");
    }
}

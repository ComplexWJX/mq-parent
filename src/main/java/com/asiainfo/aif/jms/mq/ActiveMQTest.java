package com.asiainfo.aif.jms.mq;

import com.asiainfo.aif.jms.mq.cluster.ActiveMQConsumerCluster;
import com.asiainfo.aif.jms.mq.cluster.ActiveMQProducerCluster;
import com.asiainfo.aif.jms.mq.message.MessageBean;
import com.asiainfo.aif.jms.mq.single.MyActiveMQConsumer;
import com.asiainfo.aif.jms.mq.single.MyActiveMQProducer;

public class ActiveMQTest {
    public static void main(String[] args) {
        try {
            publish();
            Thread.sleep(1000);
            subscriber();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void publish(){
        String producerName = "MyActiveMQProducer";
        MessageBean messageBean = new MessageBean();
        messageBean.setCode("1001");
        messageBean.setMessageId(1001L);
        messageBean.setText("This is a test mq message!");
//        thread(producerName,new MyActiveMQProducer(messageBean),false);
        thread(producerName,new ActiveMQProducerCluster(),false);
    }

    private static void subscriber(){
        String consumerName = "MyActiveMQConsumer";
//        thread(consumerName,new MyActiveMQConsumer(),false);
        thread(consumerName,new ActiveMQConsumerCluster(),false);
    }

    private static void thread(String threadName,Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable,threadName);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
}

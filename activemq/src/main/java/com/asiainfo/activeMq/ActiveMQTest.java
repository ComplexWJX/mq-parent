package com.asiainfo.activeMq;


import com.asiainfo.activeMq.cluster.ActiveMQConsumerCluster;
import com.asiainfo.activeMq.cluster.ActiveMQProducerCluster;
import com.asiainfo.activeMq.message.MessageBean;

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

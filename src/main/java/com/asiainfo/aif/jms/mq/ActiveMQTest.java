package com.asiainfo.aif.jms.activeMQ;

import com.asiainfo.aif.jms.activeMQ.cluster.ActiveMQConsumerCluster;
import com.asiainfo.aif.jms.activeMQ.cluster.ActiveMQProducerCluster;
import com.asiainfo.aif.jms.activeMQ.single.MyActiveMQConsumer;

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
        //thread(producerName,new MyActiveMQProducer(),false);
        thread(producerName,new ActiveMQProducerCluster(),false);
    }

    private static void subscriber(){
        String consumerName = "MyActiveMQConsumer";
        //thread(consumerName,new MyActiveMQConsumer(),false);
        thread(consumerName,new ActiveMQConsumerCluster(),false);
    }

    public static void thread(String threadName,Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable,threadName);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
}

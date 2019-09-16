package com.asiainfo.aif.jms;

public class ActiveMQTest {
    public static void main(String[] args) {
        try {
            String producerName = "MyActiveMQProducer";
            String consumerName = "MyActiveMQConsumer";
//            thread(producerName,new MyActiveMQProducer(),false);
//            thread(new MyActiveMQProducer(),false);
//            thread(new MyActiveMQProducer(),false);
            Thread.sleep(1000);
            thread(consumerName,new MyActiveMQConsumer(),false);
//            thread(new MyActiveMQConsumer(),false);
//            thread(new MyActiveMQConsumer(),false);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void thread(String threadName,Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable,threadName);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
}

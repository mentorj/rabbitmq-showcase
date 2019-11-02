package com.javaxpert.demos.rabbitmq_showcase;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests suite showing how multiple consumers behave concurrently
 * different scenarios show how connections reuse, channels reuse and  prefetch impact the throughput for consumers
 */
public class MultipleConsumersTest {
    private final static String QUEUE_NAME ="testQueue";
    private final static String EXCHANGE_NAME="testExchange";
    private final static String ROUTING_KEY ="routeMe";
    private final static int MAX_MESSAGES = 100000;
    private static Logger logger = LoggerFactory.getLogger(MultipleConsumersTest.class);

    @BeforeClass
    public void declareQueueAndBinding() {
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            ExecutorService executor = Executors.newFixedThreadPool(5);
            Runnable prepareQueue = () -> {
                try {
                    AMQP.Exchange.DeclareOk declareExchange = channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);
                    AMQP.Queue.DeclareOk declareQueue = channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                    AMQP.Queue.BindOk bindResult = channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
                    logger.info("Queue bound to exchange with result = " + bindResult.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }

            };
            prepareQueue.run();
            Thread.sleep(1000);
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

        @Test(description = "show 1 consumer with prefetch set to 100 and 1 other with prefetch set to 0",enabled = true)
    public void testWith1ProducerAnd2ConsumersSameCpu(){
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            ExecutorService executor = Executors.newFixedThreadPool(5);
            Thread.sleep(2000);
//            logger.info("now start consumers and producers");
//            Runnable publisher = () -> {
//                for(int i =0 ; i< MAX_MESSAGES;i++){
//
//                    try {
//                        Thread.sleep(2);
//                        channel.basicPublish(EXCHANGE_NAME,ROUTING_KEY,null,("Message number"+ i).getBytes());
//                        if(i%100==0){
//                            logger.debug("Messages ready to be delivered on queue: " + QUEUE_NAME+ " =" + channel.messageCount(QUEUE_NAME));
//                        }
//                    } catch (IOException | InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            };
//            executor.submit(publisher);
            Worker large = new Worker("largeCPU",50,0,QUEUE_NAME,channel,executor);
            Worker small = new Worker("smallCPU",5,0,QUEUE_NAME,channel,executor);

            executor.awaitTermination(20, TimeUnit.SECONDS);
            executor.shutdown();
            logger.info("Result is : large consumed = "+ large.getMessagesProcessed() + " Small consumed : "+ small.getMessagesProcessed());
            channel.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Error occured udring test",e);
            Assert.fail("Error during test");
        }

    }

    @Test(enabled = false)
    public void simpleTest(){
        try {
            Connection conn = new ConnectionFactory().newConnection();
            Channel channel = conn.createChannel();
            try {
                channel.basicConsume(QUEUE_NAME,(s, delivery) -> logger.debug("received message" + delivery.getBody())
                        ,
                        s -> logger.debug("cancel callback")
                        );
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

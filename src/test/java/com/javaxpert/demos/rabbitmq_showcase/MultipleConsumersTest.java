package com.javaxpert.demos.rabbitmq_showcase;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
                    channel.queuePurge(QUEUE_NAME);
                    for(int i =0;i<MAX_MESSAGES;i++){
                        String msg = "Msg number = " + i;
                        channel.basicPublish(EXCHANGE_NAME,ROUTING_KEY,null,msg.getBytes());
                    }
                    logger.debug("prepare is over now..Messages sent");
                } catch (IOException e) {
                    e.printStackTrace();
                }

            };
            Future prepare_result = executor.submit(prepareQueue);
            Thread.sleep(1000);
            logger.info("Job finished ? = "+ prepare_result.isDone() + "Queue bound & messages published");
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test(description = "basic consumer 1 thread with 1 supervisor",enabled = true)
    public void testWith1SingleConsumer(){
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            ExecutorService executor = Executors.newFixedThreadPool(5);
            //Thread.sleep(2000);
            AtomicInteger counter = new AtomicInteger(0);
            Runnable consumer = () -> {
                logger.info("starting consumer...");
                while(true){
                    try {
                        Thread.sleep(5);
                        channel.basicConsume(QUEUE_NAME, (s, delivery) ->
                                {
                                    if (counter.get() % 10000 == 0) {
                                        logger.debug("Received message = " + new String(delivery.getBody()));
                                    }
                                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                                    counter.getAndAdd(1);
                                }
                                ,
                                s -> {
                                    logger.warn("received cancelcallback");
                                }
                        );
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            Future consumer_result  = executor.submit(consumer);
            Runnable supervisor = () -> {
                logger.info("Starting supervisor");
                while(true) {
                    try {
                        Thread.sleep(25);
                        if (counter.get() == MAX_MESSAGES) {
                            logger.info("Job seems to be finished.. Halting threads.\n Future status :" + consumer_result.isDone());
                            consumer_result.cancel(true);
                        } else {
                            if (counter.get() > 50000) {
                                logger.debug("Process still busy");
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };

            Future supervisor_result = executor.submit(supervisor);
            executor.awaitTermination(10,TimeUnit.SECONDS);
            if(consumer_result.isCancelled()){
                logger.info("Consumer cancelled by supervisor");
                Assert.assertTrue(true);
            }
            logger.info("Test finished");
            channel.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Error occured udring test",e);
            Assert.fail("Error during test");
        }
    }

    @Test(description = "basic consumer 1 thread with 1 supervisor prefetch set",enabled = true)
    public void testWith1ConsumerPrefetch(){
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            channel.basicQos(100);
            ExecutorService executor = Executors.newFixedThreadPool(5);
            //Thread.sleep(2000);
            AtomicInteger counter = new AtomicInteger(0);
            double start = System.nanoTime();
            AtomicLong stop = new AtomicLong(0);
            Runnable consumer = () -> {
                logger.info("starting consumer...");
                while(true){
                    try {
                        Thread.sleep(5);
                        channel.basicConsume(QUEUE_NAME, (s, delivery) ->
                                {
                                    if (counter.get() % 10000 == 0) {
                                        logger.debug("Received message = " + new String(delivery.getBody()));
                                    }
                                    if(counter.get()==MAX_MESSAGES){
                                        stop.addAndGet(System.nanoTime());
                                    }
                                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                                    counter.getAndAdd(1);
                                }
                                ,
                                s -> {
                                    logger.warn("received cancelcallback");
                                }
                        );
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            Future consumer_result  = executor.submit(consumer);
            Runnable supervisor = () -> {
                logger.info("Starting supervisor");
                while(true) {
                    try {
                        Thread.sleep(25);
                        if (counter.get() == MAX_MESSAGES) {
                            logger.info("Job seems to be finished.. Halting threads.\n Future status :" + consumer_result.isDone());
                            consumer_result.cancel(true);
                        } else {
                            if (counter.get() > 50000) {
                                logger.debug("Process still busy");
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };

            Future supervisor_result = executor.submit(supervisor);
            executor.awaitTermination(10,TimeUnit.SECONDS);
            if(consumer_result.isCancelled()){
                logger.info("Consumer cancelled by supervisor");
                logger.info("elapsed time for test =" + ((stop.doubleValue() - start)/1000));

                Assert.assertTrue(true);
            }
            logger.info("Test finished");

        }
        catch (Exception e) {
            e.printStackTrace();
            logger.warn("Error occured udring test",e);
            Assert.fail("Error during test");
        }

    }


}

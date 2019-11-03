package com.javaxpert.demos.rabbitmq_showcase;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Suite of tests exhibiting how to use
 * Consumer properly with RabbitMQ
 */
public class RabbitConsumerDemoSuite {
    private final static String QUEUE_NAME = "testQueue";
    private final static String EXCHANGE_NAME = "testExchange";
    private final static String ROUTING_KEY = "routeMe";
    private final static int MAX_MESSAGES = 200000;
    private static Logger logger = LoggerFactory.getLogger(RabbitConsumerDemoSuite.class);

    @BeforeSuite
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
                    logger.debug("prepare is over now..Queue is now bound to exchange");
                } catch (IOException e) {
                    e.printStackTrace();
                }

            };
            Future prepare_result = executor.submit(prepareQueue);
            Thread.sleep(1000);
            channel.close();
            conn.close();
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @BeforeMethod
    public void fillQueue() {
        logger.info("Filling queue before test method");
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            for (int i = 0; i < MAX_MESSAGES; i++) {
                String msg = "Msg is number =" + i;
                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, false, false, null, msg.getBytes());
            }
         //   channel.close();
            conn.close();
            logger.info("Before method is over now");

        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Exception occured during test", e);
            Assert.fail("Exception occured", e);
        }
    }

    @Test()
    public void launch1ConsumerWithPrefetchEqualToZero() {
        logger.info("starting 1 consumer with Prefetch = 0");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("legolas");
        Connection conn = null;
        Channel channel = null;
        try {
            conn = factory.newConnection();

            channel = conn.createChannel();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicInteger counter = new AtomicInteger(0);
        Channel finalChannel1 = channel;
        Callable<Boolean> consumer = () ->
        {
            try {

                logger.info("starting consuming");
                finalChannel1.basicQos(0);
                Channel finalChannel = finalChannel1;
                while (true) {
                    Thread.sleep(25);
                    finalChannel1.basicConsume(QUEUE_NAME, false,
                            (s, delivery) -> {
                                finalChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                                counter.getAndAdd(1);
                                if(counter.get()%10000==0){
                                    logger.info("Received message = "+ new String(delivery.getBody()) +  "consumed a total of =" + counter.get());
                                }
                            },
                            s -> {
                                logger.warn("Cancel received from broker");
                            });

                }

            } catch (IOException e) {
                logger.warn("received IO exception", e);
            }
            return true;
        };
        //pool.submit(consumer);
        Callable<Boolean> supervisor = () ->
        {
            AtomicBoolean finished=new AtomicBoolean(false);
            while (!finished.get()) {
                Thread.sleep(25);
                if (counter.get() == MAX_MESSAGES) {
                    logger.info("Supervisor sees all messages as consumed");
                    finished.set(true);
                }
                if (counter.get() % 1000 == 0) {
                    logger.debug("Still busy...");
                }
            }
            return true;
        };
        //Future<Integer> supervisor_result = pool.submit(supervisor);
        try {
            Boolean results = pool.invokeAny(Arrays.asList(supervisor,consumer));
            logger.info("invokation done  with result "+ results.toString());
            channel.close();
            conn.close();
            logger.info("test finished");
        } catch (InterruptedException | IOException | TimeoutException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test()
    public void launch1ConsumerWithPrefetchGt0() {
        logger.info("starting 1 consumer with Prefetch >0");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("legolas");
        Connection conn = null;
        Channel channel = null;
        try {
            conn = factory.newConnection();
            channel = conn.createChannel();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicInteger counter = new AtomicInteger(0);
        Channel finalChannel1 = channel;
        Callable<Boolean> consumer = () ->
        {
            try {
                logger.info("starting consuming");
                finalChannel1.basicQos(25);
                Channel finalChannel = finalChannel1;
                while (true) {
                    Thread.sleep(25);
                    finalChannel1.basicConsume(QUEUE_NAME, false,
                            (s, delivery) -> {
                                finalChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
                                counter.getAndAdd(1);
                                if(counter.get()%10000==0){
                                    logger.info("Received message = "+ new String(delivery.getBody()) +  "consumed a total of =" + counter.get());
                                }
                            },
                            s -> {
                                logger.warn("Cancel received from broker");
                            });

                }

            } catch (IOException e) {
                logger.warn("received IO exception", e);
            }
            return true;
        };
        //pool.submit(consumer);
        Callable<Boolean> supervisor = () ->
        {
            AtomicBoolean finished=new AtomicBoolean(false);
            while (!finished.get()) {
                Thread.sleep(25);
                if (counter.get() == MAX_MESSAGES) {
                    logger.info("Supervisor sees all messages as consumed");
                    finished.set(true);
                }
                if (counter.get() % 1000 == 0) {
                    logger.debug("Still busy...");
                }
            }
            return true;
        };
        //Future<Integer> supervisor_result = pool.submit(supervisor);
        try {
            Boolean results = pool.invokeAny(Arrays.asList(supervisor,consumer));
            logger.info("invokation done  with result "+ results.toString());
            channel.close();
            conn.close();
            logger.info("test finished");
        } catch (InterruptedException | IOException | TimeoutException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
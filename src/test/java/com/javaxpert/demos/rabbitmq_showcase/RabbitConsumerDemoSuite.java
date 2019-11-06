package com.javaxpert.demos.rabbitmq_showcase;

import com.rabbitmq.client.*;
import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.git.GitConfigurationSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
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
    private static MainTestConfig config =null;
    @BeforeSuite
    public void declareQueueAndBinding() {
        // defines the GIT repo used to configure the application
        // in this config general properties are defined
        ConfigurationSource source = new GitConfigurationSourceBuilder()
                .withRepositoryURI("https://bitbucket.org/jmoliere/rabbbitmq-showcase-cfg.git")
                .build();
        logger.debug("adds the config git repository as a source");
        //  defines this source as the config provider
        ConfigurationProvider provider = new ConfigurationProviderBuilder().withConfigurationSource(source).build();
        // maps the config interface to the Config Interface
        config =  provider.bind("showcase", MainTestConfig.class);
        logger.debug("configuration mapped  to source git repository");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.rabbitHost());
        Connection conn;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            ExecutorService executor = Executors.newFixedThreadPool(5);
            Runnable prepareQueue = () -> {

                try {
                    AMQP.Exchange.DeclareOk declareExchange = channel.exchangeDeclare(
                            config.queueName(), BuiltinExchangeType.DIRECT, true);
                    AMQP.Queue.DeclareOk declareQueue = channel.queueDeclare(config.queueName(), true, false, false, null);
                    AMQP.Queue.BindOk bindResult = channel.queueBind(config.queueName(), config.exchangeName(), config.routingKey());
                    logger.info("Queue bound to exchange with result = " + bindResult.toString());
                    channel.queuePurge(config.queueName());
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
        factory.setHost(config.rabbitHost());
        Connection conn;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            for (int i = 0; i < config.maxMessages(); i++) {
                String msg = "Msg is number =" + i;
                channel.basicPublish(config.exchangeName(), config.routingKey(), false, false, null, msg.getBytes());
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

    @Test(description = "shows basicGet usage")
    public void showBasicget(){
        logger.info("starting basicGet demo");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.rabbitHost());
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

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger counter = new AtomicInteger(0);
        logger.info("now starting the loop");
        while(running.get()){

            try{
                Thread.sleep(config.threadDelay());
                GetResponse response =channel.basicGet(config.queueName(),true);
                counter.getAndAdd(1);
                if(counter.get()%10000==0){
                    logger.info("5% done again...");
                }
                if(counter.get()==config.maxMessages()){
                    running.set(false);
                    logger.info("halting test finished...");
                }
            }catch(Exception e){
                logger.warn("exception during test",e);
            }
        }
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("test done");
    }

    @Test(enabled=true)
    public void launch1ConsumerWithPrefetchGt0() {
        logger.info("starting 1 consumer with Prefetch >0");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.rabbitHost());
        Connection conn = null;
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger counter = new AtomicInteger(0);
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            channel.basicQos(config.prefetch()/10);
            // disable explicit ack , automatic ack is ok
            channel.basicConsume(config.queueName(), true,
                    (s, delivery) ->
                    {
                        counter.getAndAdd(1);
                        delivery.getBody();
                        if (counter.get() % 10000 == 0) {
                            logger.info("Got message: " + new String(delivery.getBody()) + " already consumed =" + counter.get() + " messages");
                        }
                        if(counter.get()==config.maxMessages()){
                            logger.info("Max messages reached , stopping the loop");
                            running.set(false);
                        }
                        //channel.basicAck(delivery,true);
                    },
                    (s, delivery) -> delivery.getReason()
            );
            while (running.get()) {
                Thread.sleep(config.threadDelay());
                if(counter.get()> (MAX_MESSAGES/2)){
                    logger.info("Consumed more than half of the messages");
                }
            }
            logger.info("test  finished");
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Error occured during test", e);
            Assert.fail("Error during test");
        }

    }

    @Test()
    public void launch1ConsumerWithPrefetchEqualsToHundred() {
        logger.info("starting 1 consumer with Prefetch=100");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.rabbitHost());
        Connection conn = null;
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger counter = new AtomicInteger(0);
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            channel.basicQos(config.prefetch());
            // disable explicit ack , automatic ack is ok
            channel.basicConsume(config.queueName(), false,
                    (s, delivery) ->
                    {
                        counter.getAndAdd(1);
                        delivery.getBody();
                        if (counter.get() % 10000 == 0) {
                            logger.info("Got message: " + new String(delivery.getBody()) + " already consumed =" + counter.get() + " messages");
                        }
                        if(counter.get()==config.maxMessages()){
                            logger.info("Max messages reached , stopping the loop");
                            running.set(false);
                        }
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(),true);
                    },
                    (s, delivery) -> delivery.getReason()
            );
            while (running.get()) {
                Thread.sleep(config.threadDelay());
                if(counter.get()> (config.maxMessages()/2)){
                    logger.info("Consumed more than half of the messages");
                }
            }
            logger.info("test  finished");
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Error occured during test", e);
            Assert.fail("Error during test");
        }

    }

    @Test(description = "consume messahes with connection reuse but  nno prefetch", enabled = true)
    public void consumeWithSameConnection() {
        logger.info("starting consume , reusing the connection to RabbitMQ");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("bilbo");
        Connection conn = null;
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger counter = new AtomicInteger(0);
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();

            // disable explicit ack , automatic ack is ok
            channel.basicConsume(config.queueName(), false,
                    (s, delivery) ->
                    {
                        counter.getAndAdd(1);
                        delivery.getBody();
                        if (counter.get() % 10000 == 0) {
                            logger.info("Got message: " + new String(delivery.getBody()) + " already consumed =" + counter.get() + " messages");
                        }
                        if(counter.get()==config.maxMessages()){
                            logger.info("Max messages reached , stopping the loop");
                            running.set(false);
                        }
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(),true);
                    },
                    (s, delivery) -> delivery.getReason()
            );
            while (running.get()) {
                Thread.sleep(config.threadDelay());
                if(counter.get()> (config.maxMessages()/2)){
                    logger.info("Consumed more than half of the messages");
                }
            }
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Error occured during test", e);
            Assert.fail("Error during test");
        }
        logger.info("finished consumer with connection reuse, consumed " + counter.get() + " messages");
    }


}

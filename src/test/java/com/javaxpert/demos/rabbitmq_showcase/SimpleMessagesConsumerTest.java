package com.javaxpert.demos.rabbitmq_showcase;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Test
public class SimpleMessagesConsumerTest {
    private final static int MESSAGES_QUEUE_VALUE = 40000;

    private final static String EXCHANGE_NAME = "test";
    private final static String ROUTING_KEY = "toto";

    private static final Logger logger = LoggerFactory.getLogger(SimpleMessagesConsumerTest.class);

    @BeforeClass
    public void setup() {
        logger.info("setup is Ok...");
    }

    @BeforeMethod
    public void fillQueue() {
        logger.info("starting filling queue with messages");
        // uses default user/passwd/host/port
        // so not specified here
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn = null;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            //use the same name for queue and exchange
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
            // declare the queue as durable no auto delete and non exclusive
            channel.queueDeclare(EXCHANGE_NAME, true, false, false, null);
            channel.queueBind(EXCHANGE_NAME, EXCHANGE_NAME, ROUTING_KEY);

            for (int i = 0; i < MESSAGES_QUEUE_VALUE; i++) {
                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, ("message" + i).getBytes());

            }
            channel.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Test(description = "consume messages without reusing channels  and connections")
    public void consumeNoReuse() {
        logger.info("starting consume , without reusing channels");
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn = null;
        try {

            for (int i = 0; i < MESSAGES_QUEUE_VALUE; i++) {
                conn = factory.newConnection();
                Channel channel = conn.createChannel();

                // auto ack messages
                channel.basicConsume(EXCHANGE_NAME, true,
                        (s, delivery) ->
                        {
                            delivery.getBody();
                            //channel.basicAck(delivery,true);
                        },
                        (s, delivery) -> delivery.getReason()
                );

                channel.close();
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        logger.info("consumer finished");
    }

    @Test(description = "consume messahes with connection reuse")
    public void consumeWithSameConnection() {
        logger.info("starting consume , reusing the connection to RabbitMQ");
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn;
        try {
            conn = factory.newConnection();
            for (int i = 0; i < MESSAGES_QUEUE_VALUE; i++) {

                Channel channel = conn.createChannel();
                channel.basicConsume(EXCHANGE_NAME, true,
                        (s, delivery) ->
                        {
                            delivery.getBody();
                            //channel.basicAck(delivery,true);
                        },
                        (s, delivery) -> delivery.getReason()
                );

                channel.close();
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("finished consumer with connection reuse");
    }

    @Test(description = "consume messages with reuse of channel and connection")
    public void consumeUsingOneChannel(){
        logger.info("starting consume , reusing the connection to RabbitMQ");
        ConnectionFactory factory = new ConnectionFactory();
        Connection conn;
        try {
            conn = factory.newConnection();
            Channel channel = conn.createChannel();
            for (int i = 0; i < MESSAGES_QUEUE_VALUE; i++) {
                channel.basicConsume(EXCHANGE_NAME, true,
                        (s, delivery) ->
                        {
                            delivery.getBody();
                            //channel.basicAck(delivery,true);
                        },
                        (s, delivery) -> delivery.getReason()
                );
                channel.close();
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("finished consumer with channel reuse");
    }
}

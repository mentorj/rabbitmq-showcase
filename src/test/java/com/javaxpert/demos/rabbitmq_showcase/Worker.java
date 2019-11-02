package com.javaxpert.demos.rabbitmq_showcase;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker class with all required characteristics
 * to launch RabbitMQ consumers:
 * queue to be consumed
 * channel to use
 * ExecutorService to register tasks...
 * Inspired freely from https://github.com/0x6e6562/hopper/blob/master/Work%20Distribution%20in%20RabbitMQ.markdown
 */
public class Worker extends DefaultConsumer {
    private int delay;
    private int prefetch;
    private String queueName;
    private Channel channel;
    private AtomicInteger messagesProcessed;
    private String workerName;
    private ExecutorService threadPool;

    private static Logger logger = LoggerFactory.getLogger(Worker.class);
    public Worker(String name,int delay, int prefetch, String queueName, Channel channel,  ExecutorService threadPool) {
        super(channel);
        workerName=name;
        this.delay = delay;
        this.prefetch = prefetch;
        this.queueName = queueName;
        this.channel = channel;
        messagesProcessed = new AtomicInteger(0);
        this.threadPool = threadPool;
        logger.debug("built a Worker ..." + toString());
        try {
            channel.basicQos(prefetch);
        } catch (IOException e) {
            logger.warn("Unable to initialise channel",e);
            throw new RuntimeException("Unable to init worker");
        }
    }

    @Override
    public String toString() {
        return "Worker{" +
                "delay=" + delay +
                ", prefetch=" + prefetch +
                ", queueName='" + queueName + '\'' +
                ", workerName='" + workerName + '\'' +
                '}';
    }

    public int getMessagesProcessed() {
        return messagesProcessed.get();
    }

    public void incrementCounter(){
        messagesProcessed.getAndAdd(1);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        super.handleDelivery(consumerTag, envelope, properties, body);
        logger.info("handleDelivery invoked");
        threadPool.submit(new MessagerHandlerTask(envelope.getDeliveryTag(),delay,channel,this));

    }
}

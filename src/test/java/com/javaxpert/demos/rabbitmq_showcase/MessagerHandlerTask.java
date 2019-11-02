package com.javaxpert.demos.rabbitmq_showcase;


import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class is used to wrap message consuming inside
 * a Runnable object. Contains details about consuming...
 * Basically sleeps, then consumes with config options set then sleep again
 */
public class MessagerHandlerTask implements Runnable {
    // delivery tag as of required by RabbitMQ client API
    private long tag;
    // how much to sleep between 2 invokes
    private  long sleep;
    // channel to use
    private Channel chan;
    // related worker , ref used to update message counter
    private Worker worker;

    private static Logger logger = LoggerFactory.getLogger(MessagerHandlerTask.class);

    public MessagerHandlerTask(long tag, long sleep, Channel chan, Worker worker) {
        this.tag = tag;
        this.sleep = sleep;
        this.chan = chan;
        this.worker = worker;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(sleep);
            if(chan.isOpen()){
                chan.basicAck(tag,false);
                worker.incrementCounter();
                logger.debug("message consumed" + worker.getMessagesProcessed());
            }
            else{
                logger.warn("Channel is closed...");
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
            logger.warn("Exception inside the MessageHandlerTask",e);
        }
    }
}

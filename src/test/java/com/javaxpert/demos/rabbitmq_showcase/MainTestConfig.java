package com.javaxpert.demos.rabbitmq_showcase;

/**
 * Interface containing most common configuration entries
 */
public interface MainTestConfig {
    /**
     * hostname for the RabbitMQ server
     * should not be the same machine as the tests runner
     * @return host or ip for the target host
     */
    public String rabbitHost();

    /**
     * TCP port for RabbitMQ
     * @return
     */
    public Integer rabbitPort();

    /**
     * Should the tests use long lived TCP connections or not
     * @return true or false
     */
    public Boolean tcpReuseConn();

    /**
     * Should the tests reuse channels
     * @return true orr false
     */
    public Boolean tcpReuseChannel();

    /**
     * specifies the size for the messages enqueued before starting tests
     * @return
     */
    public Integer maxMessages();

    /**
     * name of the queue hosting messages
     * @return
     */
    public String queueName();

    /**
     * name of the exchange used to publish messages
     * @return
     */
    public String exchangeName();

    /**
     * routing key used to deliver messages through the exchange
     * @return
     */
    public String routingKey();

    /**
     * should the queue be created as a lazy one?
     * @return
     */
    public Boolean lazyQueue();

    /**
     * prefetch size
     * @return
     */
    public Integer prefetch();

    /**
     * shoud messages be persisted
     * @return
     */
    public Boolean persistent();

    /**
     * is the queue exclusive? shoud le always false
     * @return
     */
    public Boolean exclusive();

    /**
     * is the queue flagged as autodelete ?  should be false
     * @return
     */
    public Boolean autodelete();

    /**
     * delays to sleep while consuming messages
     * @return
     */
    public Integer threadDelay();
}

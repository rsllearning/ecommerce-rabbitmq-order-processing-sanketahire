package com.ecommerce.rabbitmq.config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Manages the AMQP Connection lifecycle.
 * <p>
 * Architectural Principle (from training):
 * A TCP Connection is a heavy, expensive resource. An application process should only
 * ever open ONE physical TCP connection, shared globally across all threads.
 * From that single connection, individual lightweight Channels are created for publishers and consumers.
 */
@Component
public class RabbitMQConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQConnectionManager.class);

    private final Connection connection;

    public RabbitMQConnectionManager(
            @Value("${rabbitmq.host:localhost}") String host,
            @Value("${rabbitmq.port:5672}") int port,
            @Value("${rabbitmq.username:guest}") String username,
            @Value("${rabbitmq.password:guest}") String password,
            @Value("${rabbitmq.virtual-host:/}") String virtualHost) throws IOException, TimeoutException {

        log.info("Initializing global RabbitMQ TCP Connection to {}:{} (vhost: '{}')", host, port, virtualHost);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);

        this.connection = factory.newConnection("ecommerce-order-service");
        log.info("RabbitMQ TCP Connection established successfully: {}", connection);
    }

    /**
     * Creates a new lightweight AMQP channel.
     * Note: Channels are NOT thread-safe; each worker thread must use its own Channel.
     */
    public Channel createChannel() throws IOException {
        return this.connection.createChannel();
    }

    public Connection getConnection() {
        return this.connection;
    }

    @PreDestroy
    public void close() {
        if (connection != null && connection.isOpen()) {
            try {
                log.info("Gracefully closing global RabbitMQ TCP Connection...");
                connection.close();
            } catch (IOException e) {
                log.error("Error closing RabbitMQ connection", e);
            }
        }
    }
}

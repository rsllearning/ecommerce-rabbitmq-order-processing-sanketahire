package com.ecommerce.rabbitmq.config;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Declares the core AMQP Topology on application startup.
 * <p>
 * Ensures reproducible, idempotent infrastructure setup:
 * 1. Topic Exchange: orders_exchange
 * 2. Dead Letter Exchange (DLX): orders_dlx
 * 3. Dead Letter Queue: dead_letter_queue bound to orders_dlx
 * 4. Inventory Queue: inventory_queue (configured with x-dead-letter-exchange) bound to order.created.*
 * 5. Analytics Queue: analytics_queue bound to order.created.electronics
 */
@Configuration
public class RabbitMQTopologyConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQTopologyConfig.class);

    // Topology Constants
    public static final String ORDERS_EXCHANGE = "orders_exchange";
    public static final String ORDERS_DLX = "orders_dlx";

    public static final String INVENTORY_QUEUE = "inventory_queue";
    public static final String ANALYTICS_QUEUE = "analytics_queue";
    public static final String DEAD_LETTER_QUEUE = "dead_letter_queue";

    public static final String ROUTING_KEY_INVENTORY_BINDING = "order.created.*";
    public static final String ROUTING_KEY_ANALYTICS_BINDING = "order.created.electronics";
    public static final String ROUTING_KEY_DLX_BINDING = "#";
    public static final String DLX_ROUTING_KEY = "order.dlx";

    private final RabbitMQConnectionManager connectionManager;

    public RabbitMQTopologyConfig(RabbitMQConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @PostConstruct
    public void declareTopology() {
        log.info("Declaring RabbitMQ Exchanges, Queues, and Bindings...");

        try (Channel channel = connectionManager.createChannel()) {
            // 1. Declare Topic Exchanges (durable = true)
            // Main Topic Exchange for order routing
            channel.exchangeDeclare(ORDERS_EXCHANGE, BuiltinExchangeType.TOPIC, true);
            log.info("Declared durable Topic Exchange: '{}'", ORDERS_EXCHANGE);

            // Dead Letter Exchange (DLX) for unhandled / rejected poison pills
            channel.exchangeDeclare(ORDERS_DLX, BuiltinExchangeType.TOPIC, true);
            log.info("Declared durable Dead Letter Exchange (DLX): '{}'", ORDERS_DLX);

            // 2. Declare Dead Letter Queue (DLQ) & bind to DLX
            channel.queueDeclare(DEAD_LETTER_QUEUE, true, false, false, null);
            channel.queueBind(DEAD_LETTER_QUEUE, ORDERS_DLX, ROUTING_KEY_DLX_BINDING);
            log.info("Declared Queue '{}' and bound to DLX '{}' with binding key '{}'",
                    DEAD_LETTER_QUEUE, ORDERS_DLX, ROUTING_KEY_DLX_BINDING);

            // 3. Declare Inventory Queue with DLX configuration
            // CRITICAL: x-dead-letter-exchange forwards rejected messages (basic.reject with requeue=false)
            Map<String, Object> inventoryQueueArgs = new HashMap<>();
            inventoryQueueArgs.put("x-dead-letter-exchange", ORDERS_DLX);
            inventoryQueueArgs.put("x-dead-letter-routing-key", DLX_ROUTING_KEY);

            channel.queueDeclare(INVENTORY_QUEUE, true, false, false, inventoryQueueArgs);
            channel.queueBind(INVENTORY_QUEUE, ORDERS_EXCHANGE, ROUTING_KEY_INVENTORY_BINDING);
            log.info("Declared Queue '{}' [DLX: '{}'] and bound to '{}' with wildcard key '{}'",
                    INVENTORY_QUEUE, ORDERS_DLX, ORDERS_EXCHANGE, ROUTING_KEY_INVENTORY_BINDING);

            // 4. Declare Specialized Analytics Queue & bind with exact match key
            channel.queueDeclare(ANALYTICS_QUEUE, true, false, false, null);
            channel.queueBind(ANALYTICS_QUEUE, ORDERS_EXCHANGE, ROUTING_KEY_ANALYTICS_BINDING);
            log.info("Declared Queue '{}' and bound to '{}' with exact match key '{}'",
                    ANALYTICS_QUEUE, ORDERS_EXCHANGE, ROUTING_KEY_ANALYTICS_BINDING);

            log.info("RabbitMQ Topology successfully declared and verified!");

        } catch (IOException | java.util.concurrent.TimeoutException e) {
            log.error("Failed to declare RabbitMQ topology", e);
            throw new RuntimeException("RabbitMQ topology initialization failed", e);
        }
    }
}

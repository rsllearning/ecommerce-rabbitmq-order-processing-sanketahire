package com.ecommerce.rabbitmq.service;

import com.ecommerce.rabbitmq.config.RabbitMQConnectionManager;
import com.ecommerce.rabbitmq.config.RabbitMQTopologyConfig;
import com.ecommerce.rabbitmq.model.OrderEvent;
import com.ecommerce.rabbitmq.model.OrderRequest;
import com.ecommerce.rabbitmq.model.OrderResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Publisher service implementing asynchronous event publishing to RabbitMQ Topic Exchange.
 * <p>
 * Architectural Principles Applied:
 * 1. Decoupled & Non-blocking: Generates response immediately without waiting for downstream workers.
 * 2. Proper Message Properties: Sets messageId, deliveryMode=2 (Persistent), and contentType.
 * 3. Dynamic Topic Routing Key: Emits `order.created.<category>`.
 * 4. Channel Lifecycle: Opens a dedicated channel per operation and closes it to prevent channel leaks.
 */
@Service
public class OrderPublisherService {

    private static final Logger log = LoggerFactory.getLogger(OrderPublisherService.class);

    private final RabbitMQConnectionManager connectionManager;
    private final ObjectMapper objectMapper;

    public OrderPublisherService(RabbitMQConnectionManager connectionManager, ObjectMapper objectMapper) {
        this.connectionManager = connectionManager;
        this.objectMapper = objectMapper;
    }

    /**
     * Publishes an order_placed event to the Topic Exchange.
     */
    public OrderResponse publishOrder(OrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        return publishOrderWithId(request, orderId);
    }

    /**
     * Overloaded method allowing explicit orderId (useful for testing Idempotency).
     */
    public OrderResponse publishOrderWithId(OrderRequest request, String orderId) {
        String normalizedCategory = request.category().toLowerCase().trim();
        String routingKey = "order.created." + normalizedCategory;

        OrderEvent event = OrderEvent.create(orderId, normalizedCategory, request.quantity());

        try (Channel channel = connectionManager.createChannel()) {
            // Serialize payload to JSON bytes
            byte[] messageBytes = objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8);

            // Construct AMQP metadata properties per training specifications
            Map<String, Object> headers = new HashMap<>();
            headers.put("order_id", orderId);
            headers.put("category", normalizedCategory);
            headers.put("retry_count", 0);

            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .messageId(orderId)                         // Unique ID in properties
                    .deliveryMode(2)                            // 2 = Persistent (written to disk)
                    .contentType("application/json")            // MIME serialization format
                    .contentEncoding("UTF-8")
                    .timestamp(new Date())
                    .headers(headers)
                    .build();

            log.info("Publishing Order Event [ID: '{}', Routing Key: '{}'] to Exchange '{}'",
                    orderId, routingKey, RabbitMQTopologyConfig.ORDERS_EXCHANGE);

            // Publish directly to Topic Exchange
            channel.basicPublish(
                    RabbitMQTopologyConfig.ORDERS_EXCHANGE,
                    routingKey,
                    properties,
                    messageBytes
            );

            log.info("Order Event [ID: '{}'] successfully published to Exchange '{}'",
                    orderId, RabbitMQTopologyConfig.ORDERS_EXCHANGE);

            return OrderResponse.accepted(orderId, normalizedCategory, request.quantity(), routingKey);

        } catch (IOException | TimeoutException e) {
            log.error("Failed to publish order event to RabbitMQ", e);
            throw new RuntimeException("Error publishing order event to RabbitMQ", e);
        }
    }
}

package com.ecommerce.rabbitmq.consumer;

import com.ecommerce.rabbitmq.config.RabbitMQConnectionManager;
import com.ecommerce.rabbitmq.config.RabbitMQTopologyConfig;
import com.ecommerce.rabbitmq.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subscriber 2: Specialized Analytics Service.
 * <p>
 * Key Responsibilities & Architectural Principles:
 * 1. Binds exclusively with exact match key `order.created.electronics`.
 * 2. Processes ONLY electronics orders and ignores all other categories.
 * 3. Enforces Fair Dispatch (prefetch_count = 1) and Manual Acknowledgment (autoAck = false).
 * 4. Demonstrates Idempotent design via in-memory deduplication set.
 * 5. Logs exact required message format:
 *    "Analytics Service: Recording high-value electronics sale for order [Order ID]"
 */
@Component
public class AnalyticsConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumer.class);

    private final RabbitMQConnectionManager connectionManager;
    private final ObjectMapper objectMapper;
    private final Set<String> processedOrders = ConcurrentHashMap.newKeySet();

    private Channel channel;

    public AnalyticsConsumer(RabbitMQConnectionManager connectionManager, ObjectMapper objectMapper) {
        this.connectionManager = connectionManager;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void startConsuming() throws IOException {
        // AMQP Rule: Dedicated channel per worker
        this.channel = connectionManager.createChannel();

        // Configure Fair Dispatch (QoS)
        channel.basicQos(1);

        log.info("Analytics Service subscribed to Queue '{}' with prefetch_count=1 and manualAck=true",
                RabbitMQTopologyConfig.ANALYTICS_QUEUE);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();

            try {
                // Extract unique orderId from message properties (fallback to payload)
                String propertyOrderId = delivery.getProperties().getMessageId();
                OrderEvent event = objectMapper.readValue(delivery.getBody(), OrderEvent.class);
                String orderId = (propertyOrderId != null && !propertyOrderId.isBlank()) ? propertyOrderId : event.orderId();

                // Idempotency check
                if (!processedOrders.add(orderId)) {
                    log.warn("Analytics Service: [IDEMPOTENCY] Order [{}] was already recorded. Skipping duplicate.", orderId);
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                // Exact assignment logging format:
                // "Analytics Service: Recording high-value electronics sale for order [Order ID]"
                System.out.println("Analytics Service: Recording high-value electronics sale for order " + orderId);
                log.info("Analytics Service: Recording high-value electronics sale for order {}", orderId);

                // Manual ACK
                channel.basicAck(deliveryTag, false);

            } catch (Exception e) {
                log.error("Analytics Service: Error processing deliveryTag {}. Rejecting...", deliveryTag, e);
                channel.basicReject(deliveryTag, false);
            }
        };

        // Manual ACK mode
        channel.basicConsume(RabbitMQTopologyConfig.ANALYTICS_QUEUE, false, deliverCallback, consumerTag -> {});
    }

    @PreDestroy
    public void stop() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (Exception e) {
                log.error("Error closing AnalyticsConsumer channel", e);
            }
        }
    }
}

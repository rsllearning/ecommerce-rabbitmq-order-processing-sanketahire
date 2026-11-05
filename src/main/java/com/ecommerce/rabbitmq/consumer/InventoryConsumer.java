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
 * Subscriber 1: Inventory Service.
 * <p>
 * Key Responsibilities & Architectural Principles:
 * 1. Binds with wildcard `order.created.*` to process all orders across all categories.
 * 2. Configures Fair Dispatch (prefetch_count = 1) ensuring true load leveling.
 * 3. Enforces Manual Acknowledgment (autoAck = false) to prevent message loss on worker failure.
 * 4. Implements Idempotent Consumer design via an in-memory deduplication set.
 * 5. Handles Invalid Quantities (< 0) by explicitly rejecting WITHOUT requeuing (`requeue = false`),
 *    which safely diverts poison pills to the configured Dead Letter Exchange (orders_dlx).
 */
@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final RabbitMQConnectionManager connectionManager;
    private final ObjectMapper objectMapper;
    private final Set<String> processedOrders = ConcurrentHashMap.newKeySet();

    private Channel channel;

    public InventoryConsumer(RabbitMQConnectionManager connectionManager, ObjectMapper objectMapper) {
        this.connectionManager = connectionManager;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void startConsuming() throws IOException {
        // AMQP Rule: Dedicated channel per worker thread
        this.channel = connectionManager.createChannel();

        // 1. Fair Dispatch (QoS): Only 1 unacknowledged message in flight at a time
        channel.basicQos(1);

        log.info("Inventory Service subscribed to Queue '{}' with prefetch_count=1 and manualAck=true",
                RabbitMQTopologyConfig.INVENTORY_QUEUE);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();

            try {
                // Extract orderId from AMQP Properties (fallback to JSON payload if needed)
                String propertyOrderId = delivery.getProperties().getMessageId();
                OrderEvent event = objectMapper.readValue(delivery.getBody(), OrderEvent.class);
                String orderId = (propertyOrderId != null && !propertyOrderId.isBlank()) ? propertyOrderId : event.orderId();

                // 2. Idempotency Guard (Deduplication Pattern)
                if (!processedOrders.add(orderId)) {
                    log.warn("Inventory Service: [IDEMPOTENCY] Order [{}] was already processed. Skipping duplicate execution.", orderId);
                    // Acknowledge duplicate to remove it from queue without re-processing side effects
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                // 3. Validation & Poison Pill Avoidance
                if (event.quantity() < 0) {
                    log.error("Inventory Service: Invalid quantity ({}) for order [{}]. Explicitly rejecting without requeue (routing to DLX)...",
                            event.quantity(), orderId);
                    // CRITICAL RULE: requeue=false routes message to x-dead-letter-exchange (orders_dlx)
                    channel.basicReject(deliveryTag, false);
                    return;
                }

                // 4. Successful Business Logic Simulation
                log.info("Inventory Service: Successfully reserved inventory for order [{}] (Category: '{}', Qty: {}).",
                        orderId, event.category(), event.quantity());

                // 5. Manual Acknowledgment (ACK)
                channel.basicAck(deliveryTag, false);

            } catch (Exception e) {
                log.error("Inventory Service: Unexpected error processing message {}. Rejecting to DLX...", deliveryTag, e);
                // Reject without requeue on catastrophic parse errors to prevent poison pill loops
                channel.basicReject(deliveryTag, false);
            }
        };

        // autoAck = false ensures broker waits for explicit basicAck / basicReject
        channel.basicConsume(RabbitMQTopologyConfig.INVENTORY_QUEUE, false, deliverCallback, consumerTag -> {});
    }

    @PreDestroy
    public void stop() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (Exception e) {
                log.error("Error closing InventoryConsumer channel", e);
            }
        }
    }
}

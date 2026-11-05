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

/**
 * Subscriber 3: Error Handling via DLX (Dead Letter Exchange).
 * <p>
 * Key Responsibilities & Architectural Principles:
 * 1. Subscribes to `dead_letter_queue` bound to `orders_dlx`.
 * 2. When Inventory Service rejects an invalid order (quantity < 0) with `requeue = false`,
 *    the RabbitMQ broker intercepts and forwards it to `orders_dlx`.
 * 3. Consumes the dead-lettered message safely and logs exact assignment format:
 *    "Error Logger: Invalid order [Order ID] routed to DLX"
 * 4. Manually ACKs the message to clear it safely from the Dead Letter Queue.
 */
@Component
public class DlxErrorConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlxErrorConsumer.class);

    private final RabbitMQConnectionManager connectionManager;
    private final ObjectMapper objectMapper;

    private Channel channel;

    public DlxErrorConsumer(RabbitMQConnectionManager connectionManager, ObjectMapper objectMapper) {
        this.connectionManager = connectionManager;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void startConsuming() throws IOException {
        // AMQP Rule: Dedicated channel
        this.channel = connectionManager.createChannel();

        // Fair Dispatch QoS
        channel.basicQos(1);

        log.info("DLX Error Logger subscribed to Queue '{}' with prefetch_count=1 and manualAck=true",
                RabbitMQTopologyConfig.DEAD_LETTER_QUEUE);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();

            try {
                // Extract orderId from AMQP Properties or JSON payload
                String propertyOrderId = delivery.getProperties().getMessageId();
                OrderEvent event = objectMapper.readValue(delivery.getBody(), OrderEvent.class);
                String orderId = (propertyOrderId != null && !propertyOrderId.isBlank()) ? propertyOrderId : event.orderId();

                // Exact assignment logging format:
                // "Error Logger: Invalid order [Order ID] routed to DLX"
                System.out.println("Error Logger: Invalid order " + orderId + " routed to DLX");
                log.warn("Error Logger: Invalid order [{}] routed to DLX (Qty: {}, Category: '{}')",
                        orderId, event.quantity(), event.category());

                // Manually ACK from DLQ after auditing/logging
                channel.basicAck(deliveryTag, false);

            } catch (Exception e) {
                log.error("Error processing dead letter message {}. ACKing to prevent infinite loop.", deliveryTag, e);
                channel.basicAck(deliveryTag, false);
            }
        };

        // Manual ACK mode
        channel.basicConsume(RabbitMQTopologyConfig.DEAD_LETTER_QUEUE, false, deliverCallback, consumerTag -> {});
    }

    @PreDestroy
    public void stop() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (Exception e) {
                log.error("Error closing DlxErrorConsumer channel", e);
            }
        }
    }
}

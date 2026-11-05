package com.ecommerce.rabbitmq.model;

/**
 * Immediate HTTP response returned to the client.
 */
public record OrderResponse(
        String status,
        String orderId,
        String category,
        int quantity,
        String routingKey,
        String message,
        long timestamp
) {
    public static OrderResponse accepted(String orderId, String category, int quantity, String routingKey) {
        return new OrderResponse(
                "ACCEPTED",
                orderId,
                category,
                quantity,
                routingKey,
                "Order received and queued for asynchronous background processing.",
                System.currentTimeMillis()
        );
    }
}

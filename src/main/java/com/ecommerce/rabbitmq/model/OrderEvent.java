package com.ecommerce.rabbitmq.model;

/**
 * The event payload serialized to JSON and published to the Topic Exchange.
 */
public record OrderEvent(
        String orderId,
        String category,
        int quantity,
        String status,
        long timestamp
) {
    public static OrderEvent create(String orderId, String category, int quantity) {
        return new OrderEvent(
                orderId,
                category.toLowerCase().trim(),
                quantity,
                "PLACED",
                System.currentTimeMillis()
        );
    }
}

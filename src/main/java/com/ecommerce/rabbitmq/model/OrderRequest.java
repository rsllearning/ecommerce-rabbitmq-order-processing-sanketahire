package com.ecommerce.rabbitmq.model;

/**
 * Incoming HTTP Request payload from frontend / API client.
 */
public record OrderRequest(
        String category,
        int quantity
) {
    public OrderRequest {
        if (category == null || category.trim().isEmpty()) {
            category = "general";
        }
    }
}

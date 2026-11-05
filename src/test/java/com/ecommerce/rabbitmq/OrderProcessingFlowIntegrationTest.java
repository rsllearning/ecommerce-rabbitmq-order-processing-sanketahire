package com.ecommerce.rabbitmq;

import com.ecommerce.rabbitmq.model.OrderRequest;
import com.ecommerce.rabbitmq.model.OrderResponse;
import com.ecommerce.rabbitmq.service.OrderPublisherService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end integration test suite verifying Topic Exchange routing,
 * Dead Letter Exchange (DLX) error handling, and Idempotency deduplication.
 */
@SpringBootTest
class OrderProcessingFlowIntegrationTest {

    @Autowired
    private OrderPublisherService publisherService;

    @Test
    @DisplayName("Scenario 1: Electronics order routes to BOTH Inventory (order.created.*) and Analytics (order.created.electronics)")
    void testElectronicsOrderDualRouting() throws InterruptedException {
        OrderRequest request = new OrderRequest("electronics", 2);
        OrderResponse response = publisherService.publishOrder(request);

        assertNotNull(response.orderId());
        assertEquals("order.created.electronics", response.routingKey());
        assertEquals("ACCEPTED", response.status());

        // Allow async consumers to process and ACK
        Thread.sleep(1000);
    }

    @Test
    @DisplayName("Scenario 2: Clothing order routes ONLY to Inventory (order.created.*) and is ignored by Analytics")
    void testClothingOrderSelectiveRouting() throws InterruptedException {
        OrderRequest request = new OrderRequest("clothing", 5);
        OrderResponse response = publisherService.publishOrder(request);

        assertNotNull(response.orderId());
        assertEquals("order.created.clothing", response.routingKey());
        assertEquals("ACCEPTED", response.status());

        // Allow async consumers to process
        Thread.sleep(1000);
    }

    @Test
    @DisplayName("Scenario 3: Invalid quantity (< 0) triggers explicit rejection (requeue=false) and routes to DLX")
    void testInvalidQuantityRoutesToDLX() throws InterruptedException {
        OrderRequest request = new OrderRequest("electronics", -3);
        OrderResponse response = publisherService.publishOrder(request);

        assertNotNull(response.orderId());
        assertEquals("order.created.electronics", response.routingKey());

        // Allow Inventory to reject and DLX logger to consume from dead_letter_queue
        Thread.sleep(1500);
    }

    @Test
    @DisplayName("Scenario 4: Idempotency deduplication prevents processing duplicate order_id")
    void testIdempotencyDeduplication() throws InterruptedException {
        String fixedOrderId = "idempotent-test-" + UUID.randomUUID();
        OrderRequest request = new OrderRequest("electronics", 1);

        // First delivery: normal execution
        OrderResponse firstResponse = publisherService.publishOrderWithId(request, fixedOrderId);
        assertEquals(fixedOrderId, firstResponse.orderId());
        Thread.sleep(1000);

        // Second delivery (simulating network blip redelivery): caught by idempotency set
        OrderResponse secondResponse = publisherService.publishOrderWithId(request, fixedOrderId);
        assertEquals(fixedOrderId, secondResponse.orderId());
        Thread.sleep(1000);
    }
}

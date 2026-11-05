package com.ecommerce.rabbitmq.controller;

import com.ecommerce.rabbitmq.model.OrderRequest;
import com.ecommerce.rabbitmq.model.OrderResponse;
import com.ecommerce.rabbitmq.service.OrderPublisherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller exposing the Order placement API endpoint.
 * <p>
 * Implements Asynchronous Decoupling:
 * Returns HTTP 202 (Accepted) immediately after publishing the event to RabbitMQ,
 * preventing blocking on background worker tasks.
 */
@RestController
@RequestMapping("/api/orders")
@CrossOrigin(origins = "*")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderPublisherService publisherService;

    public OrderController(OrderPublisherService publisherService) {
        this.publisherService = publisherService;
    }

    /**
     * Endpoint to place an order.
     *
     * @param request       Order payload (category, quantity)
     * @param customOrderId Optional header for testing idempotency deduplication
     * @return HTTP 202 Accepted with OrderResponse metadata
     */
    @PostMapping
    public ResponseEntity<OrderResponse> placeOrder(
            @RequestBody OrderRequest request,
            @RequestHeader(value = "X-Order-Id", required = false) String customOrderId) {

        log.info("Received HTTP POST /api/orders request: Category='{}', Quantity={}",
                request.category(), request.quantity());

        OrderResponse response = (customOrderId != null && !customOrderId.isBlank())
                ? publisherService.publishOrderWithId(request, customOrderId.trim())
                : publisherService.publishOrder(request);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}

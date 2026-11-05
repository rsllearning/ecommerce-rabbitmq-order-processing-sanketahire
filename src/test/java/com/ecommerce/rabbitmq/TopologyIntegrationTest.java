package com.ecommerce.rabbitmq;

import com.ecommerce.rabbitmq.config.RabbitMQConnectionManager;
import com.ecommerce.rabbitmq.config.RabbitMQTopologyConfig;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class TopologyIntegrationTest {

    @Autowired
    private RabbitMQConnectionManager connectionManager;

    @Autowired
    private RabbitMQTopologyConfig topologyConfig;

    @Test
    @DisplayName("Verify that RabbitMQ topology (Exchanges, Queues, DLX) is successfully declared")
    void testTopologyDeclaration() throws Exception {
        assertNotNull(connectionManager.getConnection(), "RabbitMQ Connection must be active");

        try (Channel channel = connectionManager.createChannel()) {
            // Passive declare verifies the exchange/queue exists without modifying it
            channel.exchangeDeclarePassive(RabbitMQTopologyConfig.ORDERS_EXCHANGE);
            channel.exchangeDeclarePassive(RabbitMQTopologyConfig.ORDERS_DLX);

            channel.queueDeclarePassive(RabbitMQTopologyConfig.INVENTORY_QUEUE);
            channel.queueDeclarePassive(RabbitMQTopologyConfig.ANALYTICS_QUEUE);
            channel.queueDeclarePassive(RabbitMQTopologyConfig.DEAD_LETTER_QUEUE);
        }
    }
}

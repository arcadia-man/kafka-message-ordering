package com.example.kafkamessageordering.kafka.consumer;

import com.example.kafkamessageordering.model.OrderSyncMessage;

import tools.jackson.databind.ObjectMapper;

public abstract class AbstractOrderSyncConsumer extends AbstractKafkaConsumer<OrderSyncMessage> {
    protected AbstractOrderSyncConsumer(ObjectMapper objectMapper) {
        super(objectMapper, OrderSyncMessage.class);
    }
}
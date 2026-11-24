package com.example.kafkamessageordering.kafka.producer;

import com.example.kafkamessageordering.model.OrderSyncMessage;

public interface OrderSyncProducerStrategy extends ProducerStrategy<OrderSyncMessage> {
    // Inherits void send(String topicName, OrderSyncMessage payload);
}

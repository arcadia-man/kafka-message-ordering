package com.example.kafkamessageordering.kafka.producer;

public interface ProducerStrategy<T> {
    void send(String topicName, T payload);
}

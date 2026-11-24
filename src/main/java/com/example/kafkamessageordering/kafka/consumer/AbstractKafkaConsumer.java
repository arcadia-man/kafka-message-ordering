package com.example.kafkamessageordering.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractKafkaConsumer<T> {
    protected final ObjectMapper objectMapper;
    protected final Class<T> targetType;

    protected AbstractKafkaConsumer(ObjectMapper objectMapper, Class<T> targetType) {
        this.objectMapper = objectMapper;
        this.targetType = targetType;
    }

    protected void consumeRaw(String consumerName, String rawJson, String key, int partition, long offset) {
        try {
            T payload = objectMapper.readValue(rawJson, targetType);
            process(consumerName, payload, key, partition, offset);
        } catch (JsonProcessingException e) {
            System.err.printf("[%s] Deserialization error on Partition: %d | Offset: %d. Error: %s%n",
                    consumerName, partition, offset, e.getMessage());
        }
    }
    
    protected abstract void listen (String groupId, String rawJson, int partition, long offset, String key);

    protected abstract void process(String consumerName, T payload, String key, int partition, long offset);
}

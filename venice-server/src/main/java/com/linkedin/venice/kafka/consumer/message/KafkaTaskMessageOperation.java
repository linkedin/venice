package com.linkedin.venice.kafka.consumer.message;

/**
 * An Enum enumerating all valid messages types for a KafkaConsumptionTask.
 */
public enum KafkaTaskMessageOperation {
  SUBSCRIBE, UNSUBSCRIBE, RESET_OFFSET
}

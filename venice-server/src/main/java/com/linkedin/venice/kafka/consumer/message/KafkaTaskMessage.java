package com.linkedin.venice.kafka.consumer.message;

/**
 * Class representing Message for KafkaConsumptionTask.
 */
public class KafkaTaskMessage {

  private final KafkaTaskMessageOperation operation;
  private final String topic;
  private final int partition;

  private int attempts = 0;

  public KafkaTaskMessage(KafkaTaskMessageOperation operation, String topic, int partition) {
    this.operation = operation;
    this.topic = topic;
    this.partition = partition;
  }

  public KafkaTaskMessageOperation getOperation() {
    return operation;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public void incrementAttempt() {
    attempts ++;
  }

  public int getAttemptsCount() {
    return attempts;
  }

  @Override
  public String toString() {
    return "KafkaTaskMessage{" +
        "operation=" + operation +
        ", topic='" + topic + '\'' +
        ", partition=" + partition +
        ", attempts=" + attempts +
        '}';
  }
}

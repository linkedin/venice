package com.linkedin.venice.kafka.consumer;

/**
 * The class is used to asynchronously trigger behavior changes in the {@link StoreConsumptionTask}.
 *
 * The kinds of changes that can be triggered by a {@link ConsumerAction} are defined in the
 * {@link ConsumerActionType} enum.
 */
public class ConsumerAction {

  private final ConsumerActionType type;
  private final String topic;
  private final int partition;

  private int attempts = 0;

  public ConsumerAction(ConsumerActionType type, String topic, int partition) {
    this.type = type;
    this.topic = topic;
    this.partition = partition;
  }

  public ConsumerActionType getType() {
    return type;
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
        "type=" + type +
        ", topic='" + topic + '\'' +
        ", partition=" + partition +
        ", attempts=" + attempts +
        '}';
  }
}

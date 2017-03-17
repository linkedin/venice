package com.linkedin.venice.kafka.consumer;

import java.util.Comparator;


/**
 * The class is used to asynchronously trigger behavior changes in the {@link StoreIngestionTask}.
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

  /**
   * Create a kill consumer action. As kill action apply on all of partitions in given topic, so use 0 as partition
   * value for no meaning.
   */
  public static ConsumerAction createKillAction(String topic) {
    return new ConsumerAction(ConsumerActionType.KILL, topic, 0);
  }

  public static class ConsumerActionPriorityComparator implements Comparator<ConsumerAction> {

    @Override
    public int compare(ConsumerAction o1, ConsumerAction o2) {
      return o1.type.getActionPriority() - o2.type.getActionPriority();
    }
  }
}

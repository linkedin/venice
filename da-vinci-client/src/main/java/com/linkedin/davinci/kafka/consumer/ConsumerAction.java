package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;


/**
 * The class is used to asynchronously trigger behavior changes in the {@link StoreIngestionTask}.
 *
 * The kinds of changes that can be triggered by a {@link ConsumerAction} are defined in the
 * {@link ConsumerActionType} enum.
 */
public class ConsumerAction implements Comparable<ConsumerAction> {

  private final ConsumerActionType type;
  private final String topic;
  private final int partition;
  private final int sequenceNumber;
  private final LeaderFollowerParticipantModel.LeaderSessionIdChecker checker;

  private int attempts = 0;

  public ConsumerAction(ConsumerActionType type, String topic, int partition, int sequenceNumber) {
    this(type, topic, partition, sequenceNumber, null);
  }

  public ConsumerAction(ConsumerActionType type, String topic, int partition, int sequenceNumber,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker checker) {
    this.type = type;
    this.topic = topic;
    this.partition = partition;
    this.sequenceNumber = sequenceNumber;
    this.checker = checker;
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

  public int getSequenceNumber() {
    return sequenceNumber;
  }

  public LeaderFollowerParticipantModel.LeaderSessionIdChecker getLeaderSessionIdChecker() {
    return checker;
  }

  @Override
  public String toString() {
    return "KafkaTaskMessage{" +
        "type=" + type +
        ", topic='" + topic + '\'' +
        ", partition=" + partition +
        ", attempts=" + attempts +
        ", sequenceNumber=" + sequenceNumber +
        '}';
  }

  @Override
  public int compareTo(ConsumerAction other) {
    if (this.type.getActionPriority() == other.type.getActionPriority()) {
      /**
       * If this ConsumerAction has a smaller sequence number, which indicates it's added before the other ConsumerAction,
       * we should return negative for the `compareTo()` API indicating that this ConsumerAction is smaller and should
       * be polled first.
       */
      return this.sequenceNumber - other.sequenceNumber;
    }

    /**
     * If this ConsumerAction has a higher priority number, which indicates that it should be polled out first,
     * so we need to return negative in such case, indicating that this ConsumerAction is "smaller" and needs to be in
     * the head of the queue.
     */
    return other.type.getActionPriority() - this.type.getActionPriority();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof ConsumerAction)) {
      return false;
    }
    ConsumerAction other = (ConsumerAction) obj;

    if (topic.equals(other.topic)
        && partition == other.partition
        && sequenceNumber == other.sequenceNumber
        && type.equals(other.type)) {
      return true;
    }

    return false;
  }

  /**
   * Create a kill consumer action. As kill action apply on all of partitions in given topic, so use 0 as partition
   * value for no meaning.
   */
  public static ConsumerAction createKillAction(String topic, int sequenceNumber) {
    return new ConsumerAction(ConsumerActionType.KILL, topic, 0, sequenceNumber);
  }
}

package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import io.tehuti.utils.Utils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;


/**
 * The class is used to asynchronously trigger behavior changes in the {@link StoreIngestionTask}.
 *
 * The kinds of changes that can be triggered by a {@link ConsumerAction} are defined in the
 * {@link ConsumerActionType} enum.
 */
public class ConsumerAction implements Comparable<ConsumerAction> {
  private final ConsumerActionType type;
  private final PubSubTopicPartition topicPartition;
  private final int sequenceNumber;
  private final LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker;
  private final LeaderFollowerStateType leaderState;

  private int attempts = 0;

  private long createTimestampInMs = System.currentTimeMillis();

  private boolean isHelixTriggeredAction = true;
  private CompletableFuture<Void> future = new CompletableFuture<>();

  public ConsumerAction(
      ConsumerActionType type,
      PubSubTopicPartition topicPartition,
      int sequenceNumber,
      boolean isHelixTriggeredAction) {
    this(type, topicPartition, sequenceNumber, null, Optional.empty(), isHelixTriggeredAction);
  }

  public ConsumerAction(
      ConsumerActionType type,
      PubSubTopicPartition topicPartition,
      int sequenceNumber,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker,
      boolean isHelixTriggeredAction) {
    this(type, topicPartition, sequenceNumber, checker, Optional.empty(), isHelixTriggeredAction);
  }

  public ConsumerAction(
      ConsumerActionType type,
      PubSubTopicPartition topicPartition,
      int sequenceNumber,
      Optional<LeaderFollowerStateType> leaderState,
      boolean isHelixTriggeredAction) {
    this(type, topicPartition, sequenceNumber, null, leaderState, isHelixTriggeredAction);
  }

  private ConsumerAction(
      ConsumerActionType type,
      PubSubTopicPartition topicPartition,
      int sequenceNumber,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker checker,
      Optional<LeaderFollowerStateType> leaderState,
      boolean isHelixTriggeredAction) {
    this.type = type;
    this.topicPartition = Utils.notNull(topicPartition);
    this.sequenceNumber = sequenceNumber;
    this.checker = checker;
    this.leaderState = leaderState.orElse(LeaderFollowerStateType.STANDBY);
    this.isHelixTriggeredAction = isHelixTriggeredAction;
  }

  public ConsumerActionType getType() {
    return type;
  }

  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  public String getTopic() {
    return topicPartition.getPubSubTopic().getName();
  }

  public int getPartition() {
    return topicPartition.getPartitionNumber();
  }

  public void incrementAttempt() {
    attempts++;
  }

  public int getAttemptsCount() {
    return attempts;
  }

  public int getSequenceNumber() {
    return sequenceNumber;
  }

  public LeaderFollowerPartitionStateModel.LeaderSessionIdChecker getLeaderSessionIdChecker() {
    return checker;
  }

  public LeaderFollowerStateType getLeaderState() {
    return leaderState;
  }

  public long getCreateTimestampInMs() {
    return createTimestampInMs;
  }

  public CompletableFuture<Void> getFuture() {
    return future;
  }

  public boolean isHelixTriggeredAction() {
    return isHelixTriggeredAction;
  }

  @Override
  public String toString() {
    return "KafkaTaskMessage{" + "type=" + type + ", topicPartition=" + topicPartition + ", attempts=" + attempts
        + ", sequenceNumber=" + sequenceNumber + ", createdTimestampInMs=" + createTimestampInMs + '}';
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
    if (this == obj) {
      return true;
    }

    if (obj == null || !(obj instanceof ConsumerAction)) {
      return false;
    }
    ConsumerAction other = (ConsumerAction) obj;

    return topicPartition.equals(other.topicPartition) && sequenceNumber == other.sequenceNumber
        && type.equals(other.type) && leaderState.equals(other.leaderState);
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + topicPartition.hashCode();
    result = result * 31 + sequenceNumber;
    result = result * 31 + type.hashCode();
    result = result * 31 + leaderState.hashCode();
    return result;
  }

  /**
   * Create a kill consumer action. As kill action apply on all of partitions in given topic, so use 0 as partition
   * value for no meaning.
   */
  public static ConsumerAction createKillAction(PubSubTopic topic, int sequenceNumber) {
    return new ConsumerAction(
        ConsumerActionType.KILL,
        new PubSubTopicPartitionImpl(topic, 0),
        sequenceNumber,
        null,
        Optional.empty(),
        false);
  }
}

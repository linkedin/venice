package com.linkedin.davinci.notifier;

import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import java.util.Optional;


/**
 * RelayNotifier is a VeniceNotifier which takes in a target VeniceNotifier and forward all actions to the target notifier.
 * This RelayNotifier can served as the base implementation and other notifier implementation can override the method
 * implementation to achieve customized behaviors.
 */
public class RelayNotifier implements VeniceNotifier {
  private final VeniceNotifier targetNotifier;

  public RelayNotifier(VeniceNotifier notifier) {
    this.targetNotifier = notifier;
  }

  @Override
  public void completed(
      String kafkaTopic,
      int partitionId,
      long offset,
      String message,
      Optional<LeaderFollowerStateType> leaderState) {
    targetNotifier.completed(kafkaTopic, partitionId, offset, message, leaderState);
  }

  @Override
  public void completed(String kafkaTopic, int partitionId, long offset, String message) {
    targetNotifier.completed(kafkaTopic, partitionId, offset, message);
  }

  @Override
  public void error(String kafkaTopic, int partitionId, String message, Exception e) {
    targetNotifier.error(kafkaTopic, partitionId, message, e);
  }

  @Override
  public void started(String kafkaTopic, int partitionId, String message) {
    targetNotifier.started(kafkaTopic, partitionId, message);
  }

  @Override
  public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
    targetNotifier.restarted(kafkaTopic, partitionId, offset, message);
  }

  @Override
  public void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
    targetNotifier.endOfPushReceived(kafkaTopic, partitionId, offset, message);
  }

  @Override
  public void startOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      long offset,
      String incrementalPushVersion) {
    targetNotifier.startOfIncrementalPushReceived(kafkaTopic, partitionId, offset, incrementalPushVersion);
  }

  @Override
  public void endOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      long offset,
      String incrementalPushVersion) {
    targetNotifier.endOfIncrementalPushReceived(kafkaTopic, partitionId, offset, incrementalPushVersion);
  }

  @Override
  public void topicSwitchReceived(String kafkaTopic, int partitionId, long offset, String message) {
    targetNotifier.topicSwitchReceived(kafkaTopic, partitionId, offset, message);
  }

  @Override
  public void dataRecoveryCompleted(String kafkaTopic, int partitionId, long offset, String message) {
    targetNotifier.dataRecoveryCompleted(kafkaTopic, partitionId, offset, message);
  }

  @Override
  public void stopped(String kafkaTopic, int partitionId, long offset) {
    targetNotifier.stopped(kafkaTopic, partitionId, offset);
  }

  @Override
  public void quotaViolated(String kafkaTopic, int partitionId, long offset, String message) {
    targetNotifier.quotaViolated(kafkaTopic, partitionId, offset, message);
  }

  @Override
  public void quotaNotViolated(String kafkaTopic, int partitionId, long offset, String message) {
    targetNotifier.quotaNotViolated(kafkaTopic, partitionId, offset, message);
  }
}

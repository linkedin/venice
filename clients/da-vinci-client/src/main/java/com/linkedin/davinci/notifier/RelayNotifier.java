package com.linkedin.davinci.notifier;

import com.linkedin.venice.pubsub.api.PubSubPosition;


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
  public void completed(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
    targetNotifier.completed(kafkaTopic, partitionId, position, message);
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
  public void restarted(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
    targetNotifier.restarted(kafkaTopic, partitionId, position, message);
  }

  @Override
  public void endOfPushReceived(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
    targetNotifier.endOfPushReceived(kafkaTopic, partitionId, position, message);
  }

  @Override
  public void startOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      PubSubPosition position,
      String incrementalPushVersion) {
    targetNotifier.startOfIncrementalPushReceived(kafkaTopic, partitionId, position, incrementalPushVersion);
  }

  @Override
  public void endOfIncrementalPushReceived(
      String kafkaTopic,
      int partitionId,
      PubSubPosition position,
      String incrementalPushVersion) {
    targetNotifier.endOfIncrementalPushReceived(kafkaTopic, partitionId, position, incrementalPushVersion);
  }

  @Override
  public void topicSwitchReceived(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
    targetNotifier.topicSwitchReceived(kafkaTopic, partitionId, position, message);
  }

  @Override
  public void dataRecoveryCompleted(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
    targetNotifier.dataRecoveryCompleted(kafkaTopic, partitionId, position, message);
  }

  @Override
  public void stopped(String kafkaTopic, int partitionId, PubSubPosition position) {
    targetNotifier.stopped(kafkaTopic, partitionId, position);
  }

  @Override
  public void quotaViolated(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
    targetNotifier.quotaViolated(kafkaTopic, partitionId, position, message);
  }

  @Override
  public void quotaNotViolated(String kafkaTopic, int partitionId, PubSubPosition position, String message) {
    targetNotifier.quotaNotViolated(kafkaTopic, partitionId, position, message);
  }
}

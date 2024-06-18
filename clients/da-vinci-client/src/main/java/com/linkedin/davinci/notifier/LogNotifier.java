package com.linkedin.davinci.notifier;

import com.linkedin.venice.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Logs the Notification received.
 */
public class LogNotifier implements VeniceNotifier {
  private static final Logger LOGGER = LogManager.getLogger(LogNotifier.class);

  @Override
  public void started(String pubSubTopic, int partitionId, String message) {
    logMessage("Push started", pubSubTopic, partitionId, null, message, null);
  }

  @Override
  public void restarted(String pubSubTopic, int partitionId, long offset, String message) {
    logMessage("Push restarted", pubSubTopic, partitionId, offset, message, null);
  }

  @Override
  public void completed(String pubSubTopic, int partitionId, long offset, String message) {
    logMessage("Push completed", pubSubTopic, partitionId, offset, message, null);
  }

  @Override
  public void progress(String pubSubTopic, int partitionId, long offset, String message) {
    logMessage("Push progress", pubSubTopic, partitionId, offset, message, null);
  }

  @Override
  public void endOfPushReceived(String pubSubTopic, int partitionId, long offset, String message) {
    logMessage("Received END_OF_PUSH", pubSubTopic, partitionId, offset, message, null);
  }

  @Override
  public void topicSwitchReceived(String pubSubTopic, int partitionId, long offset, String message) {
    logMessage("Received TOPIC_SWITCH", pubSubTopic, partitionId, offset, message, null);
  }

  @Override
  public void dataRecoveryCompleted(String pubSubTopic, int partitionId, long offset, String message) {
    logMessage("Data recovery completed", pubSubTopic, partitionId, offset, message, null);
  }

  @Override
  public void startOfIncrementalPushReceived(String pubSubTopic, int partitionId, long offset, String message) {
    logMessage("Received START_OF_INCREMENTAL_PUSH", pubSubTopic, partitionId, offset, message, null);
  }

  @Override
  public void endOfIncrementalPushReceived(String pubSubTopic, int partitionId, long offset, String message) {
    logMessage("Received END_OF_INCREMENTAL_PUSH", pubSubTopic, partitionId, offset, message, null);
  }

  @Override
  public void catchUpVersionTopicOffsetLag(String pubSubTopic, int partitionId) {
    logMessage("Received CATCH_UP_BASE_TOPIC_OFFSET_LAG", pubSubTopic, partitionId, null, "", null);
  }

  private void logMessage(
      String header,
      String pubSubTopic,
      int partitionId,
      Long offset,
      String message,
      Exception ex) {
    if (ex == null) {
      LOGGER.info(
          "{} for replica: {}{}{}",
          header,
          Utils.getReplicaId(pubSubTopic, partitionId),
          pubSubTopic,
          partitionId,
          offset == null ? "" : " offset " + offset,
          (message == null || message.isEmpty()) ? "" : " message " + message);
    } else {
      LOGGER.error(
          "{} for replica: {}{}{}",
          header,
          Utils.getReplicaId(pubSubTopic, partitionId),
          offset == null ? "" : " offset " + offset,
          (message == null || message.isEmpty()) ? "" : " message " + message,
          ex);
    }
  }

  @Override
  public void close() {

  }

  @Override
  public void error(String pubSubTopic, int partitionId, String message, Exception ex) {
    logMessage("Push errored", pubSubTopic, partitionId, null, message, ex);
  }

  @Override
  public void stopped(String pubSubTopic, int partitionId, long offset) {
    logMessage("Consumption stopped", pubSubTopic, partitionId, offset, null, null);
  }
}

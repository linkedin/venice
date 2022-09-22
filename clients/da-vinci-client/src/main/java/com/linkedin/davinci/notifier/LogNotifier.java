package com.linkedin.davinci.notifier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Logs the Notification received.
 */
public class LogNotifier implements VeniceNotifier {
  private static final Logger LOGGER = LogManager.getLogger(LogNotifier.class);

  @Override
  public void started(String kafkaTopic, int partitionId, String message) {
    logMessage("Push started", kafkaTopic, partitionId, null, message, null);
  }

  @Override
  public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
    logMessage("Push restarted", kafkaTopic, partitionId, offset, message, null);
  }

  @Override
  public void completed(String kafkaTopic, int partitionId, long offset, String message) {
    logMessage("Push completed", kafkaTopic, partitionId, offset, message, null);
  }

  @Override
  public void progress(String kafkaTopic, int partitionId, long offset, String message) {
    logMessage("Push progress", kafkaTopic, partitionId, offset, message, null);
  }

  @Override
  public void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logMessage("Received END_OF_PUSH", kafkaTopic, partitionId, offset, message, null);
  }

  @Override
  public void topicSwitchReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logMessage("Received TOPIC_SWITCH", kafkaTopic, partitionId, offset, message, null);
  }

  @Override
  public void dataRecoveryCompleted(String kafkaTopic, int partitionId, long offset, String message) {
    logMessage("Data recovery completed", kafkaTopic, partitionId, offset, message, null);
  }

  @Override
  public void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logMessage("Received START_OF_INCREMENTAL_PUSH", kafkaTopic, partitionId, offset, message, null);
  }

  @Override
  public void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logMessage("Received END_OF_INCREMENTAL_PUSH", kafkaTopic, partitionId, offset, message, null);
  }

  @Override
  public void catchUpVersionTopicOffsetLag(String kafkaTopic, int partitionId) {
    logMessage("Received CATCH_UP_BASE_TOPIC_OFFSET_LAG", kafkaTopic, partitionId, null, "", null);
  }

  private void logMessage(
      String header,
      String kafkaTopic,
      int partitionId,
      Long offset,
      String message,
      Exception ex) {
    if (ex == null) {
      LOGGER.info(
          "{} for store {} user partitionId {}{}{}",
          header,
          kafkaTopic,
          partitionId,
          offset == null ? "" : " offset " + offset,
          (message == null || message.isEmpty()) ? "" : " message " + message);
    } else {
      LOGGER.error(
          "{} for store {} user partitionId {}{}{}",
          header,
          kafkaTopic,
          partitionId,
          offset == null ? "" : " offset " + offset,
          (message == null || message.isEmpty()) ? "" : " message " + message,
          ex);
    }
  }

  @Override
  public void close() {

  }

  @Override
  public void error(String kafkaTopic, int partitionId, String message, Exception ex) {
    logMessage("Push errored", kafkaTopic, partitionId, null, message, ex);
  }

  @Override
  public void stopped(String kafkaTopic, int partitionId, long offset) {
    logMessage("Consumption stopped", kafkaTopic, partitionId, offset, null, null);
  }
}

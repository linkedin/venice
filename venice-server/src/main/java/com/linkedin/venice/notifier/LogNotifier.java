package com.linkedin.venice.notifier;

import org.apache.log4j.Logger;


/**
 * Logs the Notification received.
 */
public class LogNotifier implements VeniceNotifier {

  private static final Logger logger = Logger.getLogger(LogNotifier.class);
  @Override
  public void started(String kafkaTopic, int partitionId, String message) {
    logger.info(logMessage("Push started", kafkaTopic, partitionId, null, message));
  }

  @Override
  public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
    logger.info(logMessage("Push restarted", kafkaTopic, partitionId, offset, message));
  }

  @Override
  public void completed(String kafkaTopic, int partitionId, long offset, String message) {
    logger.info(logMessage("Push completed", kafkaTopic, partitionId, offset, message));
  }

  @Override
  public void progress(String kafkaTopic, int partitionId, long offset, String message) {
    logger.info(logMessage("Push progress", kafkaTopic, partitionId, offset, message));
  }

  @Override
  public void endOfPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logger.info(logMessage("Received END_OF_PUSH", kafkaTopic, partitionId, offset, message));
  }

  @Override
  public void startOfBufferReplayReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logger.info(logMessage("Received START_OF_BUFFER_REPLAY", kafkaTopic, partitionId, offset, message));
  }

  @Override
  public void topicSwitchReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logger.info(logMessage("Received TOPIC_SWITCH", kafkaTopic, partitionId, offset, message));
  }

  @Override
  public void startOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logger.info(logMessage("Received START_OF_INCREMENTAL_PUSH", kafkaTopic, partitionId, offset, message));
  }

  @Override
  public void endOfIncrementalPushReceived(String kafkaTopic, int partitionId, long offset, String message) {
    logger.info(logMessage("Received END_OF_INCREMENTAL_PUSH", kafkaTopic, partitionId, offset, message));
  }

  @Override
  public void catchUpBaseTopicOffsetLag(String kafkaTopic, int partitionId) {
    logger.info(logMessage("Received CATCH_UP_BASE_TOPIC_OFFSET_LAG",
        kafkaTopic, partitionId, null, ""));
  }

  private String logMessage(String header, String kafkaTopic, int partitionId, Long offset, String message) {
    return String.format("%s for store %s partitionId %d%s%s", header, kafkaTopic, partitionId,
        offset == null ? "" : " offset " + offset,
        message.isEmpty()? "" : " message " + message);
  }

  @Override
  public void close() {

  }

  @Override
  public void error(String kafkaTopic, int partitionId, String message, Exception ex) {
    logger.error(logMessage("Push errored", kafkaTopic, partitionId, null, message), ex);
  }
}

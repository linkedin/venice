package com.linkedin.venice.notifier;

import org.apache.log4j.Logger;


/**
 * Logs the Notification received.
 */
public class LogNotifier implements VeniceNotifier {

  private static final Logger logger = Logger.getLogger(LogNotifier.class);
  @Override
  public void started(String storeName, int partitionId, String message) {
    logger.info(logMessage("Push started", storeName, partitionId, null, message));
  }

  @Override
  public void restarted(String storeName, int partitionId, long offset, String message) {
    logger.info(logMessage("Push restarted", storeName, partitionId, offset, message));
  }

  @Override
  public void completed(String storeName, int partitionId, long offset, String message) {
    logger.info(logMessage("Push completed", storeName, partitionId, offset, message));
  }

  @Override
  public void progress(String storeName, int partitionId, long offset, String message) {
    logger.info(logMessage("Push progress", storeName, partitionId, offset, message));
  }

  @Override
  public void endOfPushReceived(String storeName, int partitionId, long offset, String message) {
    logger.info(logMessage("Received END_OF_PUSH", storeName, partitionId, offset, message));
  }

  @Override
  public void startOfBufferReplayReceived(String storeName, int partitionId, long offset, String message) {
    logger.info(logMessage("Received START_OF_BUFFER_REPLAY", storeName, partitionId, offset, message));
  }

  @Override
  public void topicSwitchReceived(String storeName, int partitionId, long offset, String message) {
    logger.info(logMessage("Received TOPIC_SWITCH", storeName, partitionId, offset, message));
  }

  @Override
  public void startOfIncrementalPushReceived(String storeName, int partitionId, long offset, String message) {
    logger.info(logMessage("Received START_OF_INCREMENTAL_PUSH", storeName, partitionId, offset, message));
  }

  @Override
  public void endOfIncrementalPushReceived(String storeName, int partitionId, long offset, String message) {
    logger.info(logMessage("Received END_OF_INCREMENTAL_PUSH", storeName, partitionId, offset, message));
  }

  private String logMessage(String header, String storeName, int partitionId, Long offset, String message) {
    return String.format("%s for store %s partitionId %d%s%s", header, storeName, partitionId,
        offset == null ? "" : " offset " + offset,
        message.isEmpty()? "" : " message " + message);
  }

  @Override
  public void close() {

  }

  @Override
  public void error(String storeName, int partitionId, String message, Exception ex) {
    logger.error(logMessage("Push errored", storeName, partitionId, null, message)  , ex);
  }
}

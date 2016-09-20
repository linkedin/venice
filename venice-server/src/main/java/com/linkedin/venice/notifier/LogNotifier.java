package com.linkedin.venice.notifier;

import org.apache.log4j.Logger;


/**
 * Logs the Notification received.
 */
public class LogNotifier implements VeniceNotifier {

  private static final Logger logger = Logger.getLogger(LogNotifier.class);
  @Override
  public void started(String storeName, int partitionId) {
    logger.info("Push started for Store " + storeName + " partitionId " + partitionId );
  }

  @Override
  public void restarted(String storeName, int partitionId, long offset) {
    logger.info("Push restarted for store " + storeName + " partitionId " + partitionId + " offset " + offset);
  }

  @Override
  public void completed(String storeName, int partitionId, long offset) {
    logger.info("Push completed for Store " + storeName + " partitionId " + partitionId +
            " Offset " + offset);

  }

  @Override
  public void progress(String storeName, int partitionId, long offset) {
    logger.info("Push progress for Store " + storeName + " partitionId " + partitionId
            + " Offset " + offset);
  }

  @Override
  public void close() {

  }

  @Override
  public void error(String storeName, int partitionId, String message, Exception ex) {
    String errorMessage = "Push errored for Store" + storeName + " partitionId " +
            partitionId + " Message " + message;
    logger.error( errorMessage , ex);
  }
}

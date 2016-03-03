package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.consumer.VeniceNotifier;
import java.util.Arrays;
import java.util.Collection;
import org.apache.log4j.Logger;


/**
 * Logs the Notification received.
 */
public class LogNotifier implements VeniceNotifier {

  private static final Logger logger = Logger.getLogger(LogNotifier.class.getName());
  @Override
  public void started(long jobId, String storeName, int partitionId) {
    logger.info("Push started for Store " + storeName + " partitionId " + partitionId + " jobId " + jobId);
  }

  @Override
  public void completed(long jobId, String storeName, int partitionId, long counter) {
    logger.info("Push completed for Store " + storeName + " partitionId " + partitionId +
            " jobId " + jobId + " TotalMessage " + counter);

  }

  @Override
  public void progress(long jobId, String storeName, int partitionId, long counter) {
    logger.info("Push progress for Store " + storeName + " partitionId " + partitionId
            + " jobId " + jobId + " TotalMessage " + counter);
  }

  @Override
  public void close() {
  }

  @Override
  public void error(long jobId, String storeName, Collection<Integer> partitions, String message, Exception ex) {
    String errorMessage = "Push errored for Store" + storeName + " partitionIds " +
            Arrays.toString(partitions.toArray()) + " jobId " + jobId + " Message "
            + message;
    logger.error( errorMessage , ex);
  }
}

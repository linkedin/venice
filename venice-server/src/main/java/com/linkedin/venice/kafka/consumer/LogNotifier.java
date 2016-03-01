package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.consumer.VeniceNotifier;
import org.apache.log4j.Logger;


/**
 * Created by athirupa on 2/29/16.
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
}

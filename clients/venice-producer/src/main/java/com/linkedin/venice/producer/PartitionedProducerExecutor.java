package com.linkedin.venice.producer;

import com.linkedin.venice.writer.PartitionedVeniceWriteExecutor;
import io.tehuti.metrics.MetricsRepository;


/**
 * Backward-compatible name for the shared partition-striped Venice write executor.
 *
 * @deprecated Use {@link PartitionedVeniceWriteExecutor}. This shim remains for binary and source compatibility.
 */
@Deprecated
public class PartitionedProducerExecutor extends PartitionedVeniceWriteExecutor {
  public PartitionedProducerExecutor(
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      String storeName,
      MetricsRepository metricsRepository) {
    super(workerCount, workerQueueCapacity, callbackThreadCount, callbackQueueCapacity, storeName, metricsRepository);
  }
}

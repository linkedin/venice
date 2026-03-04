package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.INACTIVE_STORE_INGESTION_TASK;

import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StorageUtilizationManager;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;


/**
 * Utility methods for computing ingestion statistics from a {@link StoreIngestionTask}.
 * These methods are shared between {@link IngestionStats} (Tehuti) and
 * {@link com.linkedin.davinci.stats.ingestion.IngestionOtelStats} (OpenTelemetry).
 */
public class IngestionStatsUtils {
  private IngestionStatsUtils() {
    // Utility class, not meant to be instantiated
  }

  /**
   * Checks if the given task is active (non-null and running).
   *
   * @param task The ingestion task to check
   * @return true if the task is non-null and running, false otherwise
   */
  public static boolean hasActiveIngestionTask(StoreIngestionTask task) {
    return task != null && task.isRunning();
  }

  /**
   * Gets the ingestion task error count.
   * To prevent this metric being too noisy and align with the PreNotificationCheck of reportError,
   * this metric should only return non-zero if the ingestion task errored after EOP is received
   * for any of the partitions.
   *
   * @param task The ingestion task to get error count from
   * @return The number of failed partitions if any partition has completed, 0 otherwise
   */
  public static int getIngestionTaskErroredGauge(StoreIngestionTask task) {
    if (!hasActiveIngestionTask(task)) {
      return 0;
    }
    int totalFailedIngestionPartitions = task.getFailedIngestionPartitionCount();
    boolean anyCompleted = task.hasAnyPartitionConsumptionState(PartitionConsumptionState::isComplete);
    return anyCompleted ? totalFailedIngestionPartitions : 0;
  }

  /**
   * Gets the write compute error code from the ingestion task.
   *
   * @param task The ingestion task to get error code from
   * @return The write compute error code, or INACTIVE_STORE_INGESTION_TASK code if task is inactive
   */
  public static int getWriteComputeErrorCode(StoreIngestionTask task) {
    if (!hasActiveIngestionTask(task)) {
      return INACTIVE_STORE_INGESTION_TASK.code;
    }
    return task.getWriteComputeErrorCode();
  }

  /**
   * Gets the storage quota usage from the ingestion task.
   *
   * @param task The ingestion task to get quota usage from
   * @return The disk quota usage as a ratio (0.0-1.0), or 0 if unavailable
   */
  public static double getStorageQuotaUsed(StoreIngestionTask task) {
    if (!hasActiveIngestionTask(task)) {
      return 0;
    }
    StorageUtilizationManager storageManager = task.getStorageUtilizationManager();
    return (storageManager != null) ? storageManager.getDiskQuotaUsage() : 0;
  }
}

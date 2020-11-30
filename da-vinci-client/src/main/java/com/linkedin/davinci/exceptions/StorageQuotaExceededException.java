package com.linkedin.davinci.exceptions;

import com.linkedin.venice.exceptions.VeniceException;


public class StorageQuotaExceededException extends VeniceException {
  public StorageQuotaExceededException(String store, int partition, long currentUsage, double quota) {
    super(getErrorMessage(store, partition, currentUsage, quota));
  }

  public StorageQuotaExceededException(String store, int partition, long currentUsage, double quota, Throwable e) {
    super(getErrorMessage(store, partition, currentUsage, quota), e);
  }

  private static String getErrorMessage(String store, int partition, long currentUsage, double quota) {
    return String.format("Partition-level hybrid quota is exceeded for store %s partition %d, "
        + "the current partition usage is %s and partition disk quota is %s", store, partition, currentUsage, quota);
  }
}

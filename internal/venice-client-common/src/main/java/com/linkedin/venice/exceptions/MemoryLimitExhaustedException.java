package com.linkedin.venice.exceptions;

public class MemoryLimitExhaustedException extends VeniceException {
  public MemoryLimitExhaustedException(String storeName, int partitionId, long currentUsage) {
    super(
        "Partition: " + partitionId + " of store: " + storeName
            + "  update has already hit memory limit, and current usage: " + currentUsage);
  }

  public MemoryLimitExhaustedException(String msg) {
    super(msg);
  }
}

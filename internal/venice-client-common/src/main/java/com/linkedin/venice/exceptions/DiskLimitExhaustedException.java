package com.linkedin.venice.exceptions;

public class DiskLimitExhaustedException extends VeniceException {
  public DiskLimitExhaustedException(String storeName, int versionNumber, String diskStatus) {
    super(
        "Disk is full: throwing exception to error push: " + storeName + " version " + versionNumber + ". "
            + diskStatus);
  }

  public DiskLimitExhaustedException(String msg) {
    super(msg);
  }
}

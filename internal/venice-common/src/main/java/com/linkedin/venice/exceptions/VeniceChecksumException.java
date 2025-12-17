package com.linkedin.venice.exceptions;

public class VeniceChecksumException extends VeniceException {
  private int errorPartitionId;

  public VeniceChecksumException(String msg, int errorPartitionId) {
    super(msg);
    this.errorPartitionId = errorPartitionId;
  }

  public VeniceChecksumException(String msg, Throwable t, int errorPartitionId) {
    super(msg, t);
    this.errorPartitionId = errorPartitionId;
  }

  public int getErrorPartitionId() {
    return errorPartitionId;
  }
}

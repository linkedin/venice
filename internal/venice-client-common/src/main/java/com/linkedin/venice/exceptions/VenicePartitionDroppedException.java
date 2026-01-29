package com.linkedin.venice.exceptions;

/**
 * Exception thrown when a partition is dropped while blob transfer is in progress.
 * This signals that the blob transfer should be aborted and consumption should not start.
 */
public class VenicePartitionDroppedException extends VeniceException {
  public VenicePartitionDroppedException(String message) {
    super(message);
  }

  public VenicePartitionDroppedException(String message, Throwable cause) {
    super(message, cause);
  }
}

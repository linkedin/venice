package com.linkedin.venice.exceptions;

/**
 * Exception thrown when a blob transfer is cancelled (e.g., due to partition drop request).
 * This signals that the blob transfer should be aborted and consumption should not start.
 */
public class VeniceBlobTransferCancelledException extends VeniceException {
  public VeniceBlobTransferCancelledException(String message) {
    super(message);
  }

  public VeniceBlobTransferCancelledException(String message, Throwable cause) {
    super(message, cause);
  }
}

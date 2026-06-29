package com.linkedin.venice.exceptions;

public class ConcurrentBatchPushException extends VeniceException {
  public ConcurrentBatchPushException(String message) {
    super(message);
    super.errorType = ErrorType.CONCURRENT_BATCH_PUSH;
  }
}

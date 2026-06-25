package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class ConcurrentBatchPushException extends VeniceException {
  public ConcurrentBatchPushException(String message) {
    super(message);
    super.errorType = ErrorType.CONCURRENT_BATCH_PUSH;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_BAD_REQUEST;
  }
}

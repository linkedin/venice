package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Thrown when an admin operation's serialized size exceeds the admin topic payload cap. The
 * pre-flight raises this before allocating an execution id, so callers can safely retry with a
 * smaller payload. Returns HTTP 413 so clients can distinguish "payload too big" from generic
 * server errors.
 */
public class AdminMessageTooLargeException extends VeniceException {
  private static final long serialVersionUID = 1L;

  private final String operationName;
  private final int size;
  private final int max;

  public AdminMessageTooLargeException(String operationName, int size, int max) {
    super(
        "Admin message too large for admin topic. operation=" + operationName + ", size=" + size + ", max=" + max
            + ". Reduce the payload (e.g. shrink/chunk a large schema) or split into multiple admin operations.");
    this.operationName = operationName;
    this.size = size;
    this.max = max;
  }

  public String getOperationName() {
    return operationName;
  }

  public int getSize() {
    return size;
  }

  public int getMax() {
    return max;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_REQUEST_TOO_LONG;
  }
}

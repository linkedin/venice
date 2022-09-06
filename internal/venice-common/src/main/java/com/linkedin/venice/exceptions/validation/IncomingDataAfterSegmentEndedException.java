package com.linkedin.venice.exceptions.validation;

/**
 * This exception is thrown when the server reads data from same segment after the segment is ended.
 */
public class IncomingDataAfterSegmentEndedException extends FatalDataValidationException {
  public IncomingDataAfterSegmentEndedException(String message) {
    super(message);
  }

  public IncomingDataAfterSegmentEndedException(String message, Throwable throwable) {
    super(message, throwable);
  }
}

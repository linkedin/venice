package com.linkedin.venice.exceptions;

public class RecordTooLargeException extends VeniceException {
  public RecordTooLargeException(String message) {
    super(message);
  }

  public RecordTooLargeException(String message, Throwable cause) {
    super(message, cause);
  }
}

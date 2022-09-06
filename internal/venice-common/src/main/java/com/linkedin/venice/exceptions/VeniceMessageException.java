package com.linkedin.venice.exceptions;

/**
 * Custom Exception for Venice messages.
 * Currently everything is inherited from the superclass.
 */
public class VeniceMessageException extends VeniceException {
  private static final long serialVersionUID = 1L;

  public VeniceMessageException(String message) {
    super(message);
  }

  public VeniceMessageException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public String getMessage() {
    return super.getMessage();
  }
}

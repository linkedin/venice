package com.linkedin.venice.exceptions;

/**
 * Used when timeout happens in Venice.
 */
public class VeniceTimeoutException extends VeniceRetriableException {
  public VeniceTimeoutException(String message) {
    super(message);
  }

  public VeniceTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}

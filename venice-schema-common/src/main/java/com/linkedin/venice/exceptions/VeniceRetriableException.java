package com.linkedin.venice.exceptions;

/**
 * Interface for all Venice exceptions that are retriable.
 */
public class VeniceRetriableException extends VeniceException {
  public VeniceRetriableException(String message) {
    super(message);
  }

  public VeniceRetriableException(String message, Throwable cause) {
    super(message, cause);
  }
}

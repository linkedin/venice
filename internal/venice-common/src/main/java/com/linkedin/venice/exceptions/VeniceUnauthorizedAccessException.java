package com.linkedin.venice.exceptions;

public class VeniceUnauthorizedAccessException extends VeniceException {
  public VeniceUnauthorizedAccessException(String message) {
    super(message);
  }

  public VeniceUnauthorizedAccessException(String message, Throwable cause) {
    super(message, cause);
  }
}

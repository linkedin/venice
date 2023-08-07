package com.linkedin.venice.exceptions;

/**
 * Class for all Venice exceptions that are triggered by Kafka topic authorization related issues.
 */
public class VeniceResourceAccessException extends VeniceException {
  public VeniceResourceAccessException(String message) {
    super(message);
  }

  public VeniceResourceAccessException(String message, Throwable cause) {
    super(message, cause);
  }
}

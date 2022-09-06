package com.linkedin.venice.exceptions;

/**
 * Class for all Venice exceptions that are triggered by Kafka topic authorization related issues.
 */
public class TopicAuthorizationVeniceException extends VeniceException {
  public TopicAuthorizationVeniceException(String message) {
    super(message);
  }

  public TopicAuthorizationVeniceException(String message, Throwable cause) {
    super(message, cause);
  }
}

package com.linkedin.venice.pubsub.api.exceptions;

/**
 * Used when an operation against PubSub failed to complete in time.
 */
public class PubSubOpTimeoutException extends PubSubClientRetriableException {
  public PubSubOpTimeoutException(String message) {
    super(message);
  }

  public PubSubOpTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}

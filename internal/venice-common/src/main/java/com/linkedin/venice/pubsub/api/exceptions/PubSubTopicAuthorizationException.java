package com.linkedin.venice.pubsub.api.exceptions;

/**
 * Class for all Venice exceptions that are triggered by Kafka topic authorization related issues.
 */
public class PubSubTopicAuthorizationException extends PubSubClientException {
  public PubSubTopicAuthorizationException(String message) {
    super(message);
  }

  public PubSubTopicAuthorizationException(String message, Throwable cause) {
    super(message, cause);
  }
}

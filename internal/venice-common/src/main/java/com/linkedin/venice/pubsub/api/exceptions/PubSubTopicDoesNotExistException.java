package com.linkedin.venice.pubsub.api.exceptions;

/**
 * The source or destination topic for the replication request does not exit
 */
public class PubSubTopicDoesNotExistException extends PubSubClientRetriableException {
  public PubSubTopicDoesNotExistException(String message) {
    super(message);
  }

  public PubSubTopicDoesNotExistException(String message, Throwable cause) {
    super(message, cause);
  }
}

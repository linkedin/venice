package com.linkedin.venice.kafka;

/**
 * The source or destination topic for the replication request does not exit
 */
public class TopicDoesNotExistException extends TopicException {
  public TopicDoesNotExistException(String message) {
    super(message);
  }
}

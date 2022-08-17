package com.linkedin.venice.kafka;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Parent class for all exceptions related to Kafka topics.
 */
public abstract class TopicException extends VeniceException {
  public TopicException(String message) {
    super(message);
  }
}

package com.linkedin.venice.ingestion.control;

import com.linkedin.venice.kafka.TopicException;


/**
 * The source and destination topic for topic switching are the same topic
 */
public class DuplicateTopicException extends TopicException {
  public DuplicateTopicException(String message) {
    super(message);
  }
}

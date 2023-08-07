package com.linkedin.venice.ingestion.control;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * The source and destination topic for topic switching are the same topic
 */
public class DuplicateTopicException extends VeniceException {
  public DuplicateTopicException(String message) {
    super(message);
  }
}

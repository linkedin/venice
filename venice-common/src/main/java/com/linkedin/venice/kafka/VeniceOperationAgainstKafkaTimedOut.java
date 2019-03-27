package com.linkedin.venice.kafka;

import com.linkedin.venice.exceptions.VeniceRetriableException;


/**
 * Used when an operation against Kafka failed to complete in time.
 */
public class VeniceOperationAgainstKafkaTimedOut extends VeniceRetriableException {
  public VeniceOperationAgainstKafkaTimedOut(String message) {
    super(message);
  }

  public VeniceOperationAgainstKafkaTimedOut(String message, Throwable cause) {
    super(message, cause);
  }
}

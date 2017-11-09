package com.linkedin.venice.kafka;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Used when an operation against Kafka failed to complete in time.
 */
public class VeniceOperationAgainstKafkaTimedOut extends VeniceException {
  public VeniceOperationAgainstKafkaTimedOut(String message) {
    super(message);
  }
}

package com.linkedin.venice.exceptions;

/**
 * Custom Exception for Venice Kafka Consumer Related Issues.
 * Currently everything is inherited from the superclass.
 */
public class KafkaConsumerException extends VeniceException {
  private static final long serialVersionUID = 1L;

  public KafkaConsumerException(String message) {
    super(message);
  }

  public KafkaConsumerException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public String getMessage() {
    return super.getMessage();
  }
}

package com.linkedin.venice.kafka.consumer;

/**
 * Custom Exception for Venice Kafka Consumer Related Issues.
 * Currently everything is inherited from the superclass.
 */
public class VeniceKafkaConsumerException extends Exception {

  public VeniceKafkaConsumerException(String message) {
    super(message);
  }

  public VeniceKafkaConsumerException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public String getMessage() {
    return super.getMessage();
  }

}




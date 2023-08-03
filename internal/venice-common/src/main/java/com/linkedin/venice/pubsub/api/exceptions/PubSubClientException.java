package com.linkedin.venice.pubsub.api.exceptions;

import com.linkedin.venice.exceptions.VeniceException;


public class PubSubClientException extends RuntimeException {
  private final PubSubClientErrorCodes errorCode;

  public PubSubClientException(PubSubClientErrorCodes errorCode, String topicName, String message) {
    super(errorCode.getMessage() + " topicName: " + topicName + " - " + message);
    this.errorCode = errorCode;
  }

  public PubSubClientException(PubSubClientErrorCodes errorCode, String topicName, int partition, Throwable cause) {
    super(errorCode.getMessage() + " topicName: " + topicName + " partition: " + partition, cause);
    this.errorCode = errorCode;
  }

  public PubSubClientException(
      PubSubClientErrorCodes errorCode,
      String topicName,
      String message,
      VeniceException cause) {
    super(errorCode.getMessage() + " topicName: " + topicName + " - " + message, cause);
    this.errorCode = errorCode;
  }

  public PubSubClientException(PubSubClientErrorCodes errorCode, String topicName, VeniceException cause) {
    super(errorCode.getMessage() + " topicName: " + topicName, cause);
    this.errorCode = errorCode;
  }

  public PubSubClientException(PubSubClientErrorCodes errorCode, String topicName) {
    super(errorCode.getMessage() + " topicName: " + topicName);
    this.errorCode = errorCode;
  }

  public PubSubClientException(PubSubClientErrorCodes errorCode, String topicName, String message, Throwable cause) {
    super(errorCode.getMessage() + " topicName: " + topicName + " - " + message, cause);
    this.errorCode = errorCode;
  }

  public PubSubClientException(PubSubClientErrorCodes errorCode, String topicName, Throwable cause) {
    super(errorCode.getMessage() + " topicName: " + topicName, cause);
    this.errorCode = errorCode;
  }

  public PubSubClientErrorCodes getErrorCode() {
    return errorCode;
  }

  public static void main(String[] args) {
    System.out.println("Hello world!");
    PubSubClientErrorCodes errorCodes = PubSubClientErrorCodes.TOPIC_NOT_FOUND;
    System.out.println(PubSubClientErrorCodes.TOPIC_NOT_FOUND == errorCodes);

  }
}

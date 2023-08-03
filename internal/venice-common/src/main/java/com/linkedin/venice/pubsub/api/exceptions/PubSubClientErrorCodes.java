package com.linkedin.venice.pubsub.api.exceptions;

public enum PubSubClientErrorCodes {
  TOPIC_NOT_FOUND(1, "Topic not found"), PARTITION_NOT_FOUND(2, "Partition not found"),
  TOPIC_OR_PARTITION_NOT_FOUND(3, "Topic or partition not found"), OTHER(4, "Other error");

  private final int errorCode;
  private final String message;

  PubSubClientErrorCodes(int errorCode, String message) {
    this.errorCode = errorCode;
    this.message = message;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return message;
  }

  public static PubSubClientErrorCodes fromErrorCode(int errorCode) {
    for (PubSubClientErrorCodes code: PubSubClientErrorCodes.values()) {
      if (code.getErrorCode() == errorCode) {
        return code;
      }
    }
    return null;
  }
}

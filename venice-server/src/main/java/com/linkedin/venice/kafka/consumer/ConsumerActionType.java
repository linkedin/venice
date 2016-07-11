package com.linkedin.venice.kafka.consumer;

/**
 * An Enum enumerating all valid types of {@link ConsumerAction}.
 */
public enum ConsumerActionType {
  SUBSCRIBE, UNSUBSCRIBE, RESET_OFFSET
}

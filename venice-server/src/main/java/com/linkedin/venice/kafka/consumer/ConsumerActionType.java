package com.linkedin.venice.kafka.consumer;

/**
 * An Enum enumerating all valid types of {@link ConsumerAction}.
 */
public enum ConsumerActionType {
  SUBSCRIBE(1),
  UNSUBSCRIBE(1),
  RESET_OFFSET(1),
  KILL(2);

  private final int actionPriority;

  ConsumerActionType(int actionPriority) {
    this.actionPriority = actionPriority;
  }

  int getActionPriority() {
    return actionPriority;
  }
}

package com.linkedin.davinci.kafka.consumer;

/**
 * An Enum enumerating all valid types of {@link ConsumerAction}.
 */
public enum ConsumerActionType {
  SUBSCRIBE(1), UNSUBSCRIBE(1), RESET_OFFSET(1), PAUSE(1), RESUME(1),
  /**
   * KILL action has higher priority than others, so that once KILL action is added to the action queue,
   * we will process it immediately to avoid doing throw-away works.
   */
  KILL(2), STANDBY_TO_LEADER(1), LEADER_TO_STANDBY(1);

  /**
   * Higher number means higher priority; ConsumerAction with higher priority will be in the front
   * of the ConsumerActionsQueue and thus be processed first.
   */
  private final int actionPriority;

  ConsumerActionType(int actionPriority) {
    this.actionPriority = actionPriority;
  }

  int getActionPriority() {
    return actionPriority;
  }
}

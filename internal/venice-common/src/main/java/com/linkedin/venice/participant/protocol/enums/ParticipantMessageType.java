package com.linkedin.venice.participant.protocol.enums;

public enum ParticipantMessageType {
  KILL_PUSH_JOB(0);

  /**
   * int value of the message type
   */
  private final int value;

  ParticipantMessageType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}

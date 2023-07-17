package com.linkedin.venice.helix;

/**
 * States of Venice node in Helix.
 *
 * In order to realize zero-downtime upgrade, we assign an integer value for each of state.
 */
public enum HelixState {
  OFFLINE(3), ERROR(2), DROPPED(1), UNKNOWN(0), LEADER(12), STANDBY(11);

  private final int stateValue;

  HelixState(int stateValue) {
    this.stateValue = stateValue;
  }

  public int getStateValue() {
    return stateValue;
  }

  // In StateModelInfo and transition annotation, Only constants string is accepted, so transfer enum to String here.
  public static final String OFFLINE_STATE = "OFFLINE";
  public static final String DROPPED_STATE = "DROPPED";
  public static final String ERROR_STATE = "ERROR";
  public static final String LEADER_STATE = "LEADER";
  public static final String STANDBY_STATE = "STANDBY";
}

package com.linkedin.davinci.stats.ingestion.heartbeat;

/**
 * A simple Enum class representing all the actions for heartbeat lag monitoring setup.
 */
public enum HeartbeatLagMonitorAction {
  SET_LEADER_MONITOR("STANDBY->LEADER"), SET_FOLLOWER_MONITOR("LEADER->STANDBY"), REMOVE_MONITOR("STANDBY->OFFLINE");

  private final String trigger;

  HeartbeatLagMonitorAction(String trigger) {
    this.trigger = trigger;
  }

  public String getTrigger() {
    return trigger;
  }
}

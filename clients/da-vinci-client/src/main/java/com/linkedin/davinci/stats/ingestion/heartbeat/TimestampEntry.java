package com.linkedin.davinci.stats.ingestion.heartbeat;

public class TimestampEntry {
  /**
   * Whether this entry is for a partition which is ready to serve
   */
  public boolean readyToServe;

  /**
   * Whether this entry was consumed from input or if the system initialized it as a default entry
   */
  public boolean consumedFromUpstream;

  /**
   * The general message timestamp associated with this entry. All the lag tracking and information dump should refer to
   * this field.
   * When data message timestamp recording is enabled, this will be containing both data and heartbeat timestamp.
   * When data message timestamp recording is disabled, this will be containing heartbeat timestamp only.
   */
  private long messageTimestamp;

  /**
   * The heartbeat timestamp associated with this entry. This field is always only containing heartbeat timestamp and
   * should only be used as auxiliary information for debugging.
   */
  private long heartbeatTimestamp;

  public TimestampEntry(long heartbeatTimestamp, boolean readyToServe, boolean consumedFromUpstream) {
    this.readyToServe = readyToServe;
    this.consumedFromUpstream = consumedFromUpstream;
    this.heartbeatTimestamp = heartbeatTimestamp;
    this.messageTimestamp = heartbeatTimestamp;
  }

  public long getMessageTimestamp() {
    return messageTimestamp;
  }

  public long getHeartbeatTimestamp() {
    return heartbeatTimestamp;
  }

  public void setMessageTimestamp(long messageTimestamp) {
    this.messageTimestamp = messageTimestamp;
  }

  public void setHeartbeatTimestamp(long heartbeatTimestamp) {
    this.heartbeatTimestamp = heartbeatTimestamp;
  }
}

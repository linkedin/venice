package com.linkedin.davinci.stats.ingestion.heartbeat;

public class HeartbeatTimeStampEntry {
  /**
   * Whether this heartbeat entry is for a partition which is ready to serve
   */
  public final boolean readyToServe;

  /**
   * Whether this heartbeat entry was consumed from input or if the system initialized it as a default entry
   */
  public final boolean consumedFromUpstream;

  /**
   * The timestamp associated with this entry
   */
  public final long timestamp;

  public HeartbeatTimeStampEntry(long timestamp, boolean readyToServe, boolean consumedFromUpstream) {
    this.readyToServe = readyToServe;
    this.consumedFromUpstream = consumedFromUpstream;
    this.timestamp = timestamp;
  }
}

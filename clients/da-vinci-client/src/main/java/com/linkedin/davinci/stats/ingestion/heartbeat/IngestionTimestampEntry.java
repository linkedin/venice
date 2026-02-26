package com.linkedin.davinci.stats.ingestion.heartbeat;

/**
 * Represents a mutable timestamp entry for ingestion monitoring. This entry can track timestamps for
 * heartbeat messages as well as regular data records if record-level timestamp tracking is enabled.
 *
 * Fields are volatile to ensure visibility when written by ingestion threads and read by reporter threads.
 * Updates are performed in-place to avoid object allocations on the hot path.
 */
public class IngestionTimestampEntry {
  /**
   * Whether this timestamp entry is for a partition which is ready to serve
   */
  public volatile boolean readyToServe;

  /**
   * Whether this timestamp entry was consumed from input or if the system initialized it as a default entry
   */
  public volatile boolean consumedFromUpstream;

  /**
   * The heartbeat timestamp associated with this entry. This is the timestamp of the last
   * heartbeat control message processed for this partition.
   */
  public volatile long heartbeatTimestamp;

  /**
   * The record-level timestamp associated with this entry. This is the timestamp of the last
   * regular data record processed for this partition. Only populated if record-level timestamp
   * tracking is enabled.
   *
   * When record-level tracking is disabled, this will be the same as heartbeatTimestamp.
   */
  public volatile long recordTimestamp;

  public IngestionTimestampEntry(long timestamp, boolean readyToServe, boolean consumedFromUpstream) {
    this(timestamp, timestamp, readyToServe, consumedFromUpstream);
  }

  public IngestionTimestampEntry(
      long heartbeatTimestamp,
      long recordTimestamp,
      boolean readyToServe,
      boolean consumedFromUpstream) {
    this.heartbeatTimestamp = heartbeatTimestamp;
    this.recordTimestamp = recordTimestamp;
    this.readyToServe = readyToServe;
    this.consumedFromUpstream = consumedFromUpstream;
  }
}

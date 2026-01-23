package com.linkedin.davinci.stats.ingestion.heartbeat;

/**
 * Represents a timestamp entry for ingestion monitoring. This entry can track timestamps for
 * heartbeat messages as well as regular data records if record-level timestamp tracking is enabled.
 *
 * This class was renamed from HeartbeatTimeStampEntry to reflect its broader usage beyond just
 * heartbeat tracking.
 */
public class IngestionTimestampEntry {
  /**
   * Whether this timestamp entry is for a partition which is ready to serve
   */
  public final boolean readyToServe;

  /**
   * Whether this timestamp entry was consumed from input or if the system initialized it as a default entry
   */
  public final boolean consumedFromUpstream;

  /**
   * The heartbeat timestamp associated with this entry. This is the timestamp of the last
   * heartbeat control message processed for this partition.
   */
  public final long heartbeatTimestamp;

  /**
   * The record-level timestamp associated with this entry. This is the timestamp of the last
   * regular data record processed for this partition. Only populated if record-level timestamp
   * tracking is enabled.
   *
   * When record-level tracking is disabled, this will be the same as heartbeatTimestamp.
   */
  public final long recordTimestamp;

  /**
   * Constructor for backward compatibility with heartbeat-only tracking.
   * Sets both heartbeat and record timestamps to the same value.
   */
  public IngestionTimestampEntry(long timestamp, boolean readyToServe, boolean consumedFromUpstream) {
    this(timestamp, timestamp, readyToServe, consumedFromUpstream);
  }

  /**
   * Constructor for tracking separate heartbeat and record timestamps.
   *
   * @param heartbeatTimestamp timestamp of the last heartbeat control message
   * @param recordTimestamp timestamp of the last regular data record
   * @param readyToServe whether this partition is ready to serve
   * @param consumedFromUpstream whether this entry was consumed from input (vs initialized)
   */
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

  /**
   * For backward compatibility, return the heartbeat timestamp when accessing the generic timestamp field.
   * This ensures existing code that accesses the 'timestamp' field continues to work.
   */
  @Deprecated
  public long getTimestamp() {
    return heartbeatTimestamp;
  }
}

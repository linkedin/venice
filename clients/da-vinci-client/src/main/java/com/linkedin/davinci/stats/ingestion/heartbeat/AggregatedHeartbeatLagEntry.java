package com.linkedin.davinci.stats.ingestion.heartbeat;

public class AggregatedHeartbeatLagEntry {
  private final long currentVersionHeartbeatLag;
  private final long nonCurrentVersionHeartbeatLag;

  public AggregatedHeartbeatLagEntry(long currentVersionHeartbeatLag, long nonCurrentVersionHeartbeatLag) {
    this.currentVersionHeartbeatLag = currentVersionHeartbeatLag;
    this.nonCurrentVersionHeartbeatLag = nonCurrentVersionHeartbeatLag;
  }

  public long getCurrentVersionHeartbeatLag() {
    return currentVersionHeartbeatLag;
  }

  public long getNonCurrentVersionHeartbeatLag() {
    return nonCurrentVersionHeartbeatLag;
  }
}

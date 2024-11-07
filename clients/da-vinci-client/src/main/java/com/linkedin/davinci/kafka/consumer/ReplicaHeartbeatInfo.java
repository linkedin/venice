package com.linkedin.davinci.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class ReplicaHeartbeatInfo {
  private String replicaId;
  private String region;
  private String leaderState;
  private boolean readyToServe;
  private long heartbeat;
  private long lag;

  @JsonCreator
  public ReplicaHeartbeatInfo(
      @JsonProperty("replicaId") String replicaId,
      @JsonProperty("region") String region,
      @JsonProperty("leaderState") String leaderState,
      @JsonProperty("readyToServe") boolean readyToServe,
      @JsonProperty("heartbeat") long heartbeat,
      @JsonProperty("lag") long lag) {
    this.leaderState = leaderState;
    this.replicaId = replicaId;
    this.region = region;
    this.readyToServe = readyToServe;
    this.heartbeat = heartbeat;
    this.lag = lag;
  }

  public long getHeartbeat() {
    return heartbeat;
  }

  public void setHeartbeat(long heartbeat) {
    this.heartbeat = heartbeat;
  }

  public String getLeaderState() {
    return leaderState;
  }

  public void setLeaderState(String leaderState) {
    this.leaderState = leaderState;
  }

  public String getReplicaId() {
    return replicaId;
  }

  public void setReplicaId(String replicaId) {
    this.replicaId = replicaId;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public long getLag() {
    return lag;
  }

  public void setLag(long lag) {
    this.lag = lag;
  }

  public boolean isReadyToServe() {
    return readyToServe;
  }

  public void setReadyToServe(boolean readyToServe) {
    this.readyToServe = readyToServe;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ReplicaHeartbeatInfo replicaHeartbeatInfo = (ReplicaHeartbeatInfo) o;
    return this.replicaId.equals(replicaHeartbeatInfo.getReplicaId())
        && this.heartbeat == replicaHeartbeatInfo.getHeartbeat() && this.region.equals(replicaHeartbeatInfo.getRegion())
        && this.readyToServe == replicaHeartbeatInfo.isReadyToServe() && this.lag == replicaHeartbeatInfo.getLag()
        && this.leaderState.equals(replicaHeartbeatInfo.leaderState);
  }

  @Override
  public int hashCode() {
    int result = replicaId.hashCode();
    result = 31 * result + region.hashCode();
    result = 31 * result + leaderState.hashCode();
    result = 31 * result + Boolean.hashCode(readyToServe);
    result = 31 * result + Long.hashCode(heartbeat);
    result = 31 * result + Long.hashCode(lag);
    return result;
  }

  @Override
  public String toString() {
    return "{" + "\"replica\"='" + replicaId + '\'' + ", \"region\"='" + region + '\'' + ", \"leaderState\"='"
        + leaderState + '\'' + ", \"readyToServe\"='" + readyToServe + '\'' + ", \"heartbeat\"=" + heartbeat
        + ", \"lag\"=" + lag + '}';
  }
}

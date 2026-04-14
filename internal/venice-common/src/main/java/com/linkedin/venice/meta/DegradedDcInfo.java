package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;


/**
 * Represents metadata about a datacenter that has been marked as degraded.
 * Stored in a separate ZK node per cluster, not in LiveClusterConfig.
 */
public class DegradedDcInfo {
  @JsonProperty("timestamp")
  private long timestamp;

  @JsonProperty("timeoutMinutes")
  private int timeoutMinutes;

  @JsonProperty("operatorId")
  private String operatorId;

  public DegradedDcInfo() {
  }

  public DegradedDcInfo(long timestamp, int timeoutMinutes, String operatorId) {
    this.timestamp = timestamp;
    this.timeoutMinutes = timeoutMinutes;
    this.operatorId = operatorId;
  }

  public DegradedDcInfo(DegradedDcInfo clone) {
    this.timestamp = clone.timestamp;
    this.timeoutMinutes = clone.timeoutMinutes;
    this.operatorId = clone.operatorId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getTimeoutMinutes() {
    return timeoutMinutes;
  }

  public void setTimeoutMinutes(int timeoutMinutes) {
    this.timeoutMinutes = timeoutMinutes;
  }

  public String getOperatorId() {
    return operatorId;
  }

  public void setOperatorId(String operatorId) {
    this.operatorId = operatorId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DegradedDcInfo that = (DegradedDcInfo) o;
    return timestamp == that.timestamp && timeoutMinutes == that.timeoutMinutes
        && Objects.equals(operatorId, that.operatorId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, timeoutMinutes, operatorId);
  }

  @Override
  public String toString() {
    return "DegradedDcInfo{timestamp=" + timestamp + ", timeoutMinutes=" + timeoutMinutes + ", operatorId='"
        + operatorId + "'}";
  }
}

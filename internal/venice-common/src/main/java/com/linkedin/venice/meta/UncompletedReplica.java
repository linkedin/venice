package com.linkedin.venice.meta;

import com.linkedin.venice.pushmonitor.ExecutionStatus;


public class UncompletedReplica {
  private String instanceId;
  private ExecutionStatus status;
  private long currentOffset;
  private String statusDetails;

  public UncompletedReplica() {
  }

  public UncompletedReplica(String instanceId, ExecutionStatus status, long currentOffset, String statusDetails) {
    this.instanceId = instanceId;
    this.status = status;
    this.currentOffset = currentOffset;
    this.statusDetails = statusDetails;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public ExecutionStatus getStatus() {
    return status;
  }

  public void setStatus(ExecutionStatus status) {
    this.status = status;
  }

  public long getCurrentOffset() {
    return currentOffset;
  }

  public void setCurrentOffset(long currentOffset) {
    this.currentOffset = currentOffset;
  }

  public String getStatusDetails() {
    return statusDetails;
  }

  public void setStatusDetails(String statusDetails) {
    this.statusDetails = statusDetails;
  }

  @Override
  public String toString() {
    return "ReplicaDetail{" + "instanceId=" + instanceId + ", status=" + status.toString() + ", currentOffset="
        + currentOffset + ", statusDetails=" + statusDetails + "}";
  }
}

package com.linkedin.venice.routerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.Objects;


public class ReplicaState {
  private int partitionId;
  private String participantId;
  private ExecutionStatus venicePushStatus;

  public ReplicaState() {
    // Dummy constructor for JSON
  }

  public ReplicaState(int partitionId, String participantId, ExecutionStatus venicePushStatus) {
    this.partitionId = partitionId;
    this.participantId = participantId;
    this.venicePushStatus = venicePushStatus;
  }

  public int getPartition() {
    return partitionId;
  }

  public String getParticipantId() {
    return participantId;
  }

  public ExecutionStatus getVenicePushStatus() {
    return venicePushStatus;
  }

  @JsonIgnore
  public boolean isReadyToServe() {
    return venicePushStatus == ExecutionStatus.COMPLETED;
  }

  public void setPartition(int partitionId) {
    this.partitionId = partitionId;
  }

  public void setParticipantId(String participantId) {
    this.participantId = participantId;
  }

  public void setVenicePushStatus(String venicePushStatus) {
    if (venicePushStatus != null) {
      this.venicePushStatus = ExecutionStatus.valueOf(venicePushStatus);
    }
  }

  public void setVenicePushStatus(ExecutionStatus venicePushStatus) {
    this.venicePushStatus = venicePushStatus;
  }

  @JsonIgnore
  public String toString() {
    return "{" + partitionId + ", " + participantId + ", " + venicePushStatus + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ReplicaState that = (ReplicaState) o;
    return this.partitionId == that.partitionId && Objects.equals(this.participantId, that.participantId)
        && this.venicePushStatus == that.venicePushStatus;
  }

  @Override
  public int hashCode() {
    int result = this.partitionId;
    result = 31 * result + Objects.hashCode(this.participantId);
    result = 31 * result + Objects.hashCode(this.venicePushStatus);
    return result;
  }
}

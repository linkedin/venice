package com.linkedin.venice.routerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class ReplicaState {
  private int partitionId;
  private String participantId;
  private String externalViewStatus;
  private String venicePushStatus;
  private boolean isReadyToServe;

  public ReplicaState() {
    // Dummy constructor for JSON
  }

  public ReplicaState(int partitionId, String participantId, String externalViewStatus, String venicePushStatus,
      boolean isReadyToServe) {
    this.partitionId = partitionId;
    this.participantId = participantId;
    this.externalViewStatus = externalViewStatus;
    this.venicePushStatus = venicePushStatus;
    this.isReadyToServe = isReadyToServe;
  }

  public int getPartition() {
    return partitionId;
  }

  public String getParticipantId() {
    return participantId;
  }

  public String getExternalViewStatus() {
    return externalViewStatus;
  }

  public String getVenicePushStatus() {
    return venicePushStatus;
  }

  public boolean isReadyToServe() {
    return isReadyToServe;
  }

  public void setPartition(int partitionId) {
    this.partitionId = partitionId;
  }

  public void setParticipantId(String participantId) {
    this.participantId = participantId;
  }

  public void setExternalViewStatus(String externalViewStatus) {
    this.externalViewStatus = externalViewStatus;
  }

  public void setVenicePushStatus(String venicePushStatus) {
    this.venicePushStatus = venicePushStatus;
  }

  public void setReadyToServe(boolean readyToServe) {
    isReadyToServe = readyToServe;
  }

  @JsonIgnore
  public String toString() {
    return "{" + partitionId + ", " + participantId + ", " + externalViewStatus + ", " + venicePushStatus + ", "
        + isReadyToServe + "}";
  }
}

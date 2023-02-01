package com.linkedin.venice.meta;

public class ReplicaDetail {
  private String instanceId;
  private String pushStartDateTime;
  private String pushEndDateTime;

  public ReplicaDetail() {
  }

  public ReplicaDetail(String instanceId, String pushStartDateTime, String pushEndDateTime) {
    this.instanceId = instanceId;
    this.pushStartDateTime = pushStartDateTime;
    this.pushEndDateTime = pushEndDateTime;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public String getPushStartDateTime() {
    return pushStartDateTime;
  }

  public void setPushStartDateTime(String pushStartDateTime) {
    this.pushStartDateTime = pushStartDateTime;
  }

  public String getPushEndDateTime() {
    return pushEndDateTime;
  }

  public void setPushEndDateTime(String pushEndDateTime) {
    this.pushEndDateTime = pushEndDateTime;
  }

  @Override
  public String toString() {
    return "ReplicaDetail{" + "instanceId='" + instanceId + '\'' + ", pushStartDateTime='" + pushStartDateTime + '\''
        + ", pushEndDateTime='" + pushEndDateTime + '\'' + '}';
  }
}

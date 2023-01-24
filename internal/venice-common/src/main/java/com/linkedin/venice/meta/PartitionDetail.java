package com.linkedin.venice.meta;

import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;


public class PartitionDetail {
  private int partitionId;
  private List<ReplicaDetail> replicaDetails = new ArrayList<>();

  public PartitionDetail() {
  }

  public PartitionDetail(int partitionId) {
    this.partitionId = partitionId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public List<ReplicaDetail> getReplicaDetails() {
    return replicaDetails;
  }

  public void setReplicaDetails(List<ReplicaDetail> replicaDetails) {
    this.replicaDetails = replicaDetails;
  }

  void addReplicaDetails(final PartitionStatus pStatus) {
    for (Map.Entry<String, ReplicaStatus> entry: pStatus.getReplicaStatusMap().entrySet()) {
      String instanceId = entry.getKey();
      StatusSnapshot startStatus = entry.getValue().findStartStatus();
      StatusSnapshot completeStatus = entry.getValue().findCompleteStatus();
      replicaDetails.add(
          new ReplicaDetail(
              instanceId,
              startStatus != null ? startStatus.getTime() : StringUtils.EMPTY,
              completeStatus != null ? completeStatus.getTime() : StringUtils.EMPTY));
    }
  }

  @Override
  public String toString() {
    return "PartitionDetail{" + "partitionId=" + partitionId + ", replicaDetails=" + replicaDetails + '}';
  }
}

class ReplicaDetail {
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

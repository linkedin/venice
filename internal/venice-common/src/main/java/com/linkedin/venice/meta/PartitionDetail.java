package com.linkedin.venice.meta;

import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReplicaStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.List;
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
    for (ReplicaStatus replicaStatus: pStatus.getReplicaStatuses()) {
      String instanceId = replicaStatus.getInstanceId();
      Pair<StatusSnapshot, StatusSnapshot> status = replicaStatus.findStartedAndCompletedStatus();
      String startedTime = (status == null) ? StringUtils.EMPTY : status.getFirst().getTime();
      String completedTime = (status == null) ? StringUtils.EMPTY : status.getSecond().getTime();
      replicaDetails.add(new ReplicaDetail(instanceId, startedTime, completedTime));
    }
  }

  @Override
  public String toString() {
    return "PartitionDetail{" + "partitionId=" + partitionId + ", replicaDetails=" + replicaDetails + '}';
  }
}

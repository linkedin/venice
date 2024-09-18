package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_CREATED;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.utils.Utils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Class stores the status of one partition including all the replicas statuses in this partition.
 */
public class PartitionStatus implements Comparable<PartitionStatus> {
  private final int partitionId;

  private final Map<String, ReplicaStatus> replicaStatusMap;

  @JsonCreator
  public PartitionStatus(@JsonProperty("partitionId") int partitionId) {
    this.partitionId = partitionId;
    this.replicaStatusMap = new HashMap<>();
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void updateReplicaStatus(String instanceId, ExecutionStatus newStatus) {
    updateReplicaStatus(instanceId, newStatus, "", true);
  }

  public void updateReplicaStatus(String instanceId, ExecutionStatus newStatus, boolean enableStatusHistory) {
    updateReplicaStatus(instanceId, newStatus, "", enableStatusHistory);
  }

  public void updateReplicaStatus(
      String instanceId,
      ExecutionStatus newStatus,
      String incrementalPushVersion,
      long progress) {
    ReplicaStatus replicaStatus = updateReplicaStatus(instanceId, newStatus, incrementalPushVersion, true);
    replicaStatus.setCurrentProgress(progress);
  }

  public void batchUpdateReplicaIncPushStatus(String instanceId, List<String> incPushVersionList, long progress) {
    ReplicaStatus replicaStatus = null;
    for (String incrementalPushVersion: incPushVersionList) {
      replicaStatus = updateReplicaStatus(
          instanceId,
          ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
          incrementalPushVersion,
          true);
    }
    if (replicaStatus != null) {
      replicaStatus.setCurrentProgress(progress);
    }
  }

  private ReplicaStatus updateReplicaStatus(
      String instanceId,
      ExecutionStatus newStatus,
      String incrementalPushVersion,
      boolean enableStatusHistory) {
    ReplicaStatus replicaStatus =
        replicaStatusMap.compute(instanceId, (k, v) -> v == null ? new ReplicaStatus(k, enableStatusHistory) : v);
    replicaStatus.setIncrementalPushVersion(incrementalPushVersion);
    replicaStatus.updateStatus(newStatus);
    return replicaStatus;
  }

  public Collection<ReplicaStatus> getReplicaStatuses() {
    return Collections.unmodifiableCollection(replicaStatusMap.values());
  }

  @SuppressWarnings("unused") // Used by zk serialize and deserialize
  public void setReplicaStatuses(Collection<ReplicaStatus> replicaStatuses) {
    replicaStatusMap.clear();
    for (ReplicaStatus replicaStatus: replicaStatuses) {
      replicaStatusMap.put(replicaStatus.getInstanceId(), replicaStatus);
    }
  }

  public ExecutionStatus getReplicaStatus(String instanceId) {
    ReplicaStatus replicaStatus = replicaStatusMap.get(instanceId);
    if (replicaStatus == null) {
      return NOT_CREATED;
    }
    return replicaStatus.getCurrentStatus();
  }

  public List<StatusSnapshot> getReplicaHistoricStatusList(String instanceId) {
    ReplicaStatus replicaStatus = replicaStatusMap.get(instanceId);
    if (replicaStatus == null) {
      return Collections.emptyList();
    }
    return replicaStatus.getStatusHistory();
  }

  public boolean hasFatalDataValidationError() {
    for (ReplicaStatus replicaStatus: replicaStatusMap.values()) {
      if (ExecutionStatus.isError(replicaStatus.getCurrentStatus()) && replicaStatus.getIncrementalPushVersion() != null
          && replicaStatus.getIncrementalPushVersion().contains(Utils.FATAL_DATA_VALIDATION_ERROR)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionStatus that = (PartitionStatus) o;

    if (partitionId != that.partitionId) {
      return false;
    }
    return replicaStatusMap.equals(that.replicaStatusMap);
  }

  @Override
  public int hashCode() {
    int result = partitionId;
    result = 31 * result + replicaStatusMap.hashCode();
    return result;
  }

  @Override
  public int compareTo(PartitionStatus o) {
    return this.partitionId - o.partitionId;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, ReplicaStatus> entry: replicaStatusMap.entrySet()) {
      sb.append(entry.getKey()).append(":").append(entry.getValue().getCurrentStatus().name()).append(" ");
    }
    return sb.toString();
  }
}

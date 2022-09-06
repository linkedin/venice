package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;

import com.linkedin.venice.exceptions.VeniceException;
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

  private Map<String, ReplicaStatus> replicaStatusMap;

  public PartitionStatus(int partitionId) {
    this.partitionId = partitionId;
    replicaStatusMap = new HashMap<>();
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void updateReplicaStatus(String instanceId, ExecutionStatus newStatus) {
    updateReplicaStatus(instanceId, newStatus, "");
  }

  public void updateReplicaStatus(String instanceId, ExecutionStatus newStatus, String incrementalPushVersion) {
    ReplicaStatus replicaStatus = replicaStatusMap.get(instanceId);
    if (replicaStatus == null) {
      replicaStatus = new ReplicaStatus(instanceId);
      replicaStatusMap.put(instanceId, replicaStatus);
    }
    replicaStatus.setIncrementalPushVersion(incrementalPushVersion);
    replicaStatus.updateStatus(newStatus);
  }

  public void updateProgress(String instanceId, long progress) {
    if (replicaStatusMap.containsKey(instanceId)) {
      replicaStatusMap.get(instanceId).setCurrentProgress(progress);
    } else {
      throw new VeniceException("Can not find replica status for: " + instanceId);
    }
  }

  public void updateIncrementalPushVersion(String instanceId, String metadata) {
    if (replicaStatusMap.containsKey(instanceId)) {
      replicaStatusMap.get(instanceId).setIncrementalPushVersion(metadata);
    } else {
      throw new VeniceException("Can not find replica status for: " + instanceId);
    }
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
    if (replicaStatusMap.containsKey(instanceId)) {
      return replicaStatusMap.get(instanceId).getCurrentStatus();
    } else {
      return NOT_CREATED;
    }
  }

  public List<StatusSnapshot> getReplicaHistoricStatusList(String instanceId) {
    if (replicaStatusMap.containsKey(instanceId)) {
      return replicaStatusMap.get(instanceId).getStatusHistory();
    }

    return Collections.emptyList();
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

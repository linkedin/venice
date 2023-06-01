package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Collection;


public class ReadOnlyPartitionStatus extends PartitionStatus {
  public ReadOnlyPartitionStatus(int partitionId, Collection<ReplicaStatus> replicaStatuses) {
    super(partitionId);
    this.setReplicaStatuses(replicaStatuses);
  }

  @Override
  public void updateReplicaStatus(String instanceId, ExecutionStatus newStatus) {
    throw new VeniceException("Unsupported operation in ReadonlyPartition status: updateReplicaStatus.");
  }

  @Override
  public void updateReplicaStatus(String instanceId, ExecutionStatus newStatus, boolean enableStatusHistory) {
    throw new VeniceException("Unsupported operation in ReadonlyPartition status: updateProgress.");
  }

  @Override
  public void updateReplicaStatus(
      String instanceId,
      ExecutionStatus newStatus,
      String incrementalPushVersion,
      long progress) {
    throw new VeniceException("Unsupported operation in ReadonlyPartition status: updateProgress.");
  }

  public static ReadOnlyPartitionStatus fromPartitionStatus(PartitionStatus partitionStatus) {
    return new ReadOnlyPartitionStatus(partitionStatus.getPartitionId(), partitionStatus.getReplicaStatuses());
  }
}

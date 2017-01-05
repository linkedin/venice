package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import java.util.Collection;


public class ReadonlyPartitionStatus extends PartitionStatus {
  public ReadonlyPartitionStatus(int partitionId, Collection<ReplicaStatus> replicaStatuses) {
    super(partitionId);
    this.setReplicaStatuses(replicaStatuses);
  }

  @Override
  public void updateReplicaStatus(String instanceId, ExecutionStatus newStatus) {
    throw new VeniceException("Unsupported operation in ReadonlyPartition status: updateReplicaStatus.");
  }

  @Override
  public void updateProgress(String instanceId, long progress) {
    throw new VeniceException("Unsupported operation in ReadonlyPartition status: updateProgress.");
  }

  public static ReadonlyPartitionStatus fromPartitionStatus(PartitionStatus partitionStatus) {
    return new ReadonlyPartitionStatus(partitionStatus.getPartitionId(), partitionStatus.getReplicaStatuses());
  }
}

package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is for wrapping the information about the role of the partition replica on that host to
 *  {@link AggKafkaConsumerService} to achieve finer granularity of consumer assignment. Those information should be
 *  triggered by store version role (future, current and backup), workload type and leader or follower state. Version role
 *  and workload type information are properly managed by {@link StoreIngestionTask} to be sent to
 *  {@link AggKafkaConsumerService}. We could add more information regarding this partition replica if needed in the future.
 */

public class PartitionReplicaIngestionContext {
  private static final Logger LOGGER = LogManager.getLogger(PartitionReplicaIngestionContext.class);
  private final PubSubTopicPartition pubSubTopicPartition;
  private final PubSubTopic versionTopic;
  private final VersionRole versionRole;
  private final WorkloadType workloadType;
  private final boolean isReadyToServe;

  public enum VersionRole {
    CURRENT, BACKUP, FUTURE
  }

  // TODO: Add more workload types if needed, here we only care about active active or write compute workload.
  public enum WorkloadType {
    AA_OR_WRITE_COMPUTE, NON_AA_OR_WRITE_COMPUTE
  }

  public PartitionReplicaIngestionContext(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition,
      VersionRole versionRole,
      WorkloadType workloadType) {
    this(versionTopic, pubSubTopicPartition, versionRole, workloadType, true);
  }

  public PartitionReplicaIngestionContext(
      PubSubTopic versionTopic,
      PubSubTopicPartition pubSubTopicPartition,
      VersionRole versionRole,
      WorkloadType workloadType,
      boolean isReadyToServe) {
    this.versionTopic = versionTopic;
    this.pubSubTopicPartition = pubSubTopicPartition;
    this.versionRole = versionRole;
    this.workloadType = workloadType;
    this.isReadyToServe = isReadyToServe;
  }

  public VersionRole getVersionRole() {
    return versionRole;
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    return pubSubTopicPartition;
  }

  public PubSubTopic getVersionTopic() {
    return versionTopic;
  }

  public WorkloadType getWorkloadType() {
    return workloadType;
  }

  public boolean isReadyToServe() {
    return isReadyToServe;
  }

  public static WorkloadType determineWorkloadType(
      boolean isActiveActiveReplicationEnabled,
      boolean isWriteComputationEnabled) {
    if (isWriteComputationEnabled || isActiveActiveReplicationEnabled) {
      return WorkloadType.AA_OR_WRITE_COMPUTE;
    }
    return WorkloadType.NON_AA_OR_WRITE_COMPUTE;
  }

  public static VersionRole determineStoreVersionRole(int versionNumber, int currentVersionNumber) {
    if (currentVersionNumber < versionNumber) {
      return VersionRole.FUTURE;
    }
    if (currentVersionNumber > versionNumber) {
      return VersionRole.BACKUP;
    }
    return VersionRole.CURRENT;
  }
}

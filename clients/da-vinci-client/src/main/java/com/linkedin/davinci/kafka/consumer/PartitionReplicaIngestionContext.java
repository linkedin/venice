package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
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
  private final Boolean isLeader;
  private final PubSubTopicPartition pubSubTopicPartition;
  private final PubSubTopic versionTopic;
  private final VersionRole versionRole;
  private final WorkloadType workloadType;

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
      Boolean isLeader,
      VersionRole versionRole,
      WorkloadType workloadType) {
    this.versionTopic = versionTopic;
    this.pubSubTopicPartition = pubSubTopicPartition;
    this.isLeader = isLeader;
    this.versionRole = versionRole;
    this.workloadType = workloadType;
  }

  public boolean isLeaderReplica() {
    return isLeader;
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

  public static WorkloadType getWorkloadType(PubSubTopic versionTopic, Store store) {
    checkVersionTopicStoreOrThrow(versionTopic, store);
    int versionNumber = Version.parseVersionFromKafkaTopicName(versionTopic.getName());
    Version version = store.getVersion(versionNumber);
    if (version == null) {
      return WorkloadType.NON_AA_OR_WRITE_COMPUTE;
    }
    if (store.isWriteComputationEnabled() || version.isActiveActiveReplicationEnabled()) {
      return WorkloadType.AA_OR_WRITE_COMPUTE;
    }
    return WorkloadType.NON_AA_OR_WRITE_COMPUTE;
  }

  public static VersionRole getStoreVersionRole(PubSubTopic versionTopic, Store store) {
    checkVersionTopicStoreOrThrow(versionTopic, store);
    int versionNumber = Version.parseVersionFromKafkaTopicName(versionTopic.getName());
    int currentVersionNumber = store.getCurrentVersion();
    if (currentVersionNumber < versionNumber) {
      return VersionRole.FUTURE;
    } else if (currentVersionNumber > versionNumber) {
      return VersionRole.BACKUP;
    } else {
      return VersionRole.CURRENT;
    }
  }

  private static void checkVersionTopicStoreOrThrow(PubSubTopic versionTopic, Store store) {
    if (store == null) {
      LOGGER.error("Invalid store meta-data for {}", versionTopic);
      throw new VeniceNoStoreException(versionTopic.getStoreName());
    }

    if (!store.getName().equals(versionTopic.getStoreName())) {
      throw new VeniceException(
          "Store name mismatch for store " + store.getName() + " and version topic " + versionTopic);
    }
  }

}

package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


/**
 * This class is for passing the information regarding the role of the partition replica on that host to
 *  {@link AggKafkaConsumerService} to achieve finer granularity of consumer assignment. Those information should be
 *  triggered by helix state transition and properly managed by {@link StoreIngestionTask} to be sent to
 *  {@link AggKafkaConsumerService}. Currently, the role of future/current/backup for version and leader/follower for
 *  partition are supported. We could add more information regarding this partition replica if needed in the future.
 */

public class TopicPartitionReplicaRole {
  private final Boolean isLeader;
  private final PubSubTopicPartition pubSubTopicPartition;
  private final PubSubTopic versionTopic;
  private final VersionRole versionRole;

  public enum VersionRole {
    CURRENT, BACKUP, FUTURE
  }

  public TopicPartitionReplicaRole(
      Boolean isLeader,
      VersionRole versionRole,
      PubSubTopicPartition pubSubTopicPartition,
      PubSubTopic versionTopic) {
    this.isLeader = isLeader;
    this.versionRole = versionRole;
    this.pubSubTopicPartition = pubSubTopicPartition;
    this.versionTopic = versionTopic;
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
}

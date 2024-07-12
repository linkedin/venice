package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


public class TopicPartitionReplicaRole {
  Boolean isLeader;
  Boolean isCurrentVersion;
  PubSubTopicPartition pubSubTopicPartition;
  PubSubTopic versionTopic;

  VersionRole versionRole;

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

  public Boolean isCurrentVersion() {
    return isCurrentVersion;
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    return pubSubTopicPartition;
  }

  public PubSubTopic getVersionTopic() {
    return versionTopic;
  }
}

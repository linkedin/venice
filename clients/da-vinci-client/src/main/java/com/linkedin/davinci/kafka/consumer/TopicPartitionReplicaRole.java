package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


public class TopicPartitionReplicaRole {
  Boolean isLeader;
  Boolean isCurrentVersion;
  PubSubTopicPartition pubSubTopicPartition;
  PubSubTopic versionTopic;

  public TopicPartitionReplicaRole(
      Boolean isLeader,
      Boolean isCurrentVersion,
      PubSubTopicPartition pubSubTopicPartition,
      PubSubTopic versionTopic) {
    this.isLeader = isLeader;
    this.isCurrentVersion = isCurrentVersion;
    this.pubSubTopicPartition = pubSubTopicPartition;
    this.versionTopic = versionTopic;
  }

  public void setLeaderRole(Boolean isLeader) {
    this.isLeader = isLeader;
  }

  public void setCurrentVersionRole(Boolean currentVersion) {
    this.isCurrentVersion = currentVersion;
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

package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubTopic;


public class PubSubTopicPartitionInfo {
  private final PubSubTopic pubSubTopic;
  private final int partition;
  private final Boolean hasInSyncReplica;

  public PubSubTopicPartitionInfo(PubSubTopic pubSubTopic, int partition, Boolean hasInSyncReplica) {
    this.pubSubTopic = pubSubTopic;
    this.partition = partition;
    this.hasInSyncReplica = hasInSyncReplica;
  }

  public PubSubTopic topic() {
    return pubSubTopic;
  }

  /**
   * The partition id
   */
  public int partition() {
    return partition;
  }

  public Boolean hasInSyncReplicas() {
    return hasInSyncReplica;
  }

  @Override
  public String toString() {
    return String
        .format("Partition(topic = %s, partition=%s, hasInSyncReplica = %s)", pubSubTopic, partition, hasInSyncReplica);
  }
}

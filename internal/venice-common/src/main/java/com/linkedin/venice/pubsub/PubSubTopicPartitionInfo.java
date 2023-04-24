package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubTopic;


public class PubSubTopicPartitionInfo {
  private final PubSubTopic pubSubTopic;
  private final int partition;
  private final int replicasNum;
  private final Boolean hasInSyncReplica;

  public PubSubTopicPartitionInfo(PubSubTopic pubSubTopic, int partition, int replicasNum, Boolean hasInSyncReplica) {
    this.pubSubTopic = pubSubTopic;
    this.partition = partition;
    this.replicasNum = replicasNum;
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

  public int replicasNum() {
    return replicasNum;
  }

  @Override
  public String toString() {
    return String.format(
        "Partition(topic = %s, partition=%s, replicasInfo = %s, hasInSyncReplica = %s)",
        pubSubTopic,
        partition,
        replicasNum,
        hasInSyncReplica);
  }
}

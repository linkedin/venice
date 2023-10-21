package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


public class PubSubTopicPartitionInfo {
  private final PubSubTopicPartition topicPartition;
  private final Boolean hasInSyncReplica;

  public PubSubTopicPartitionInfo(PubSubTopic pubSubTopic, int partition, Boolean hasInSyncReplica) {
    this.topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partition);
    this.hasInSyncReplica = hasInSyncReplica;
  }

  public PubSubTopic topic() {
    return topicPartition.getPubSubTopic();
  }

  /**
   * The partition id
   */
  public int partition() {
    return topicPartition.getPartitionNumber();
  }

  public Boolean hasInSyncReplicas() {
    return hasInSyncReplica;
  }

  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public String toString() {
    return String
        .format("Partition(topic = %s, partition=%s, hasInSyncReplica = %s)", topic(), partition(), hasInSyncReplica);
  }
}

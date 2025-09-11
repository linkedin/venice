package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.utils.Utils;


/**
 * A simple implementation of PubSubProduceResult interface for testing purposes.
 */
public class SimplePubSubProduceResultImpl implements PubSubProduceResult {
  private final String topic;
  private final int partition;
  private final PubSubPosition pubSubPosition;
  private final int serializedSize;

  public SimplePubSubProduceResultImpl(String topic, int partition, PubSubPosition pubSubPosition, int serializedSize) {
    this.topic = topic;
    this.partition = partition;
    this.pubSubPosition = pubSubPosition;
    this.serializedSize = serializedSize;
  }

  @Override
  public PubSubPosition getPubSubPosition() {
    return pubSubPosition;
  }

  @Override
  public int getSerializedSize() {
    return serializedSize;
  }

  @Override
  public String getTopic() {
    return topic;
  }

  @Override
  public int getPartition() {
    return partition;
  }

  @Override
  public String toString() {
    return "[TP: " + Utils.getReplicaId(topic, partition) + ", pubSubPosition: " + pubSubPosition + ", serializedSize: "
        + serializedSize + "]";
  }
}

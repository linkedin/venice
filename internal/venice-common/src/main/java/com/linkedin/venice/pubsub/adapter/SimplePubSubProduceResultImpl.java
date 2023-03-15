package com.linkedin.venice.pubsub.adapter;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;


/**
 * A simple implementation of PubSubProduceResult interface for testing purposes.
 */
public class SimplePubSubProduceResultImpl implements PubSubProduceResult {
  private final String topic;
  private final int partition;
  private final long offset;
  private final int serializedSize;

  public SimplePubSubProduceResultImpl(String topic, int partition, long offset, int serializedSize) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.serializedSize = serializedSize;
  }

  @Override
  public long getOffset() {
    return offset;
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
    return "[Topic: " + topic + "," + "Partition: " + partition + "," + "Offset: " + offset + "," + "SerializedSize: "
        + serializedSize + "]";
  }
}

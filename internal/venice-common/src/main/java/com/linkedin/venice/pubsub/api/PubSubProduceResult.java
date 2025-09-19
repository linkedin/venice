package com.linkedin.venice.pubsub.api;

/**
 * An interface implemented by specific PubSubProducerAdapters to return the result of a produce action.
 */
public interface PubSubProduceResult {
  /**
   * The position of the record in the topic/partition.
   */
  PubSubPosition getPubSubPosition();

  /**
   * Sum of the size of the serialized, uncompressed key and value in bytes.
   */
  int getSerializedSize();

  /**
   * The topic the record was appended to
   */
  String getTopic();

  /**
   * The partition the record was sent to
   */
  int getPartition();
}

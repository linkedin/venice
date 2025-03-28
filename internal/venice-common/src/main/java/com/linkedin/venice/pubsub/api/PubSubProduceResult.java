package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.annotation.UnderDevelopment;


/**
 * An interface implemented by specific PubSubProducerAdapters to return the result of a produce action.
 */
public interface PubSubProduceResult {
  /**
   * The offset of the record in the topic/partition.
   */
  long getOffset();

  /**
   * The position of the record in the topic/partition.
   */
  @UnderDevelopment
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

package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.pubsub.PubSubPositionUtils;


/**
 * Represents a position of a message in a partition of a topic.
 */
public interface PubSubPosition {
  int comparePosition(PubSubPosition other);

  long diff(PubSubPosition other);

  byte[] toBytes();

  boolean equals(Object obj);

  int hashCode();

  static PubSubPosition fromBytes(byte[] bytes) {
    return PubSubPositionUtils.fromBytes(bytes);
  }
}

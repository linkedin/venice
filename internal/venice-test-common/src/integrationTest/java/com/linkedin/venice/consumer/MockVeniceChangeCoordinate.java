package com.linkedin.venice.consumer;

import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.venice.pubsub.api.PubSubPosition;


public class MockVeniceChangeCoordinate extends VeniceChangeCoordinate {
  String topic;

  public MockVeniceChangeCoordinate() {
    super();
  }

  public MockVeniceChangeCoordinate(String topic, PubSubPosition pubSubPosition, Integer partition) {
    this(topic, pubSubPosition, partition, VeniceChangeCoordinate.UNDEFINED_CONSUMER_SEQUENCE_ID);
  }

  public MockVeniceChangeCoordinate(
      String topic,
      PubSubPosition pubSubPosition,
      Integer partition,
      long consumerSequenceId) {
    super(topic, pubSubPosition, partition, consumerSequenceId);
    this.topic = topic;
  }

  @Override
  protected String getTopic() {
    return this.topic;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    MockVeniceChangeCoordinate that = (MockVeniceChangeCoordinate) obj;
    return this.topic.equals(that.topic) && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + topic.hashCode();
  }
}

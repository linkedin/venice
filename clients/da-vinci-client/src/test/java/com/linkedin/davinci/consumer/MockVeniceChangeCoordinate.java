package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.api.PubSubPosition;


public class MockVeniceChangeCoordinate extends VeniceChangeCoordinate {
  String topic;

  public MockVeniceChangeCoordinate(String topic, PubSubPosition pubSubPosition, Integer partition) {
    super(topic, pubSubPosition, partition);
    this.topic = topic;
  }

  @Override
  protected String getTopic() {
    return this.topic;
  }
}

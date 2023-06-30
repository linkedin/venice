package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicType;


public class TestPubSubTopic implements PubSubTopic {
  String topicName;
  String storeName;
  PubSubTopicType topicType;

  public TestPubSubTopic(String topicName, String storeName, PubSubTopicType topicType) {
    this.topicName = topicName;
    this.storeName = storeName;
    this.topicType = topicType;
  }

  @Override
  public String getName() {
    return topicName;
  }

  @Override
  public PubSubTopicType getPubSubTopicType() {
    return topicType;
  }

  @Override
  public String getStoreName() {
    return storeName;
  }
}

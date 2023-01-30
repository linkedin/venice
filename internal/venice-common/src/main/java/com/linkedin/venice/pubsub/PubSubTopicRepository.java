package com.linkedin.venice.pubsub;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


public class PubSubTopicRepository {
  LoadingCache<String, PubSubTopic> topicCache =
      Caffeine.newBuilder().maximumSize(2000).build(topicName -> new PubSubTopicImpl(topicName));

  public PubSubTopic getTopic(String topicName) {
    return topicCache.get(topicName);
  }
  public PubSubTopicPartition getPubSubTopicPartition(String topicName, int partition) {
    return new PubSubTopicPartitionImpl(getTopic(topicName), partition);
  }
}

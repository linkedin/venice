package com.linkedin.venice.pubsub;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;


public class PubSubTopicRepository {
  private final LoadingCache<String, PubSubTopic> topicCache =
      Caffeine.newBuilder().maximumSize(2000).build(PubSubTopicImpl::new);

  private final LoadingCache<String, Int2ObjectMap<PubSubTopicPartition>> partitionCache =
      Caffeine.newBuilder().maximumSize(2000).build(k -> new Int2ObjectOpenHashMap<>());

  public PubSubTopic getTopic(String topicName) {
    return topicCache.get(topicName);
  }

  public PubSubTopicPartition getTopicPartition(String topicName, int partitionId) {
    return getTopicPartition(getTopic(topicName), partitionId);
  }

  public PubSubTopicPartition getTopicPartition(PubSubTopic topic, int partitionId) {
    // Caffeine LoadingCache.get() never returns null — it throws if the loader returns null.
    // The null check satisfies SpotBugs NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE.
    Int2ObjectMap<PubSubTopicPartition> partitionMap = partitionCache.get(topic.getName());
    if (partitionMap == null) {
      throw new IllegalStateException("Unexpected null from LoadingCache for topic: " + topic.getName());
    }
    synchronized (partitionMap) {
      return partitionMap.computeIfAbsent(partitionId, id -> new PubSubTopicPartitionImpl(topic, id));
    }
  }
}

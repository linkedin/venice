package com.linkedin.venice.pubsub;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;


public class PubSubTopicRepository {
  private final LoadingCache<String, PubSubTopic> topicCache =
      Caffeine.newBuilder().maximumSize(2000).build(PubSubTopicImpl::new);

  private final LoadingCache<PubSubTopic, Int2ObjectMap<PubSubTopicPartition>> partitionCache =
      Caffeine.newBuilder().maximumSize(2000).build(this::createPartitionMap);

  private Int2ObjectMap<PubSubTopicPartition> createPartitionMap(PubSubTopic topic) {
    return Int2ObjectMaps.synchronize(new Int2ObjectOpenHashMap<>());
  }

  public PubSubTopic getTopic(String topicName) {
    return topicCache.get(topicName);
  }

  public PubSubTopicPartition getTopicPartition(String topicName, int partitionId) {
    return getTopicPartition(getTopic(topicName), partitionId);
  }

  public PubSubTopicPartition getTopicPartition(PubSubTopic topic, int partitionId) {
    PubSubTopic canonicalTopic = getTopic(topic.getName());
    Int2ObjectMap<PubSubTopicPartition> partitionMap = partitionCache.get(canonicalTopic);
    synchronized (partitionMap) {
      return partitionMap.computeIfAbsent(partitionId, id -> new PubSubTopicPartitionImpl(canonicalTopic, id));
    }
  }
}
